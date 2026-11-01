"""Streamlit interface and LangGraph runtime for OrderOps Control Tower."""

import asyncio
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

# Trace export must never sit on the user-response critical path. These values
# are set before MLflow is imported because MLflow reads them during startup.
os.environ.setdefault("MLFLOW_ENABLE_ASYNC_TRACE_LOGGING", "true")
os.environ.setdefault("MLFLOW_ASYNC_TRACE_LOGGING_RETRY_TIMEOUT", "10")

import mlflow
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    DatabricksMCPServer,
    DatabricksMultiServerMCPClient,
)
from databricks_mcp import DatabricksMCPClient
from langchain.agents import create_agent

from guardrails import check_input, classify_question, mask_sensitive_text, sql_identifier

# Streamlit requires page configuration to be the first UI command. It appears
# before cached resource creation so startup is valid on every Streamlit version
# allowed by requirements.txt.
st.set_page_config(page_title="OrderOps Control Tower", page_icon="📦", layout="wide")

# -----------------------------------------------------------------------------
# Runtime configuration
# -----------------------------------------------------------------------------
# Databricks Apps injects these values from the governed resources in app.yaml.
# Required values use os.environ so a missing binding fails clearly at startup.
DATABRICKS_WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
MCP_APP_NAME = os.environ["ORDEROPS_MCP_APP_NAME"]
LLM_ENDPOINT = os.environ["DATABRICKS_LLM_ENDPOINT"]
MLFLOW_EXPERIMENT_ID = os.getenv("MLFLOW_EXPERIMENT_ID", "")
AGENT_AUDIT_TABLE = sql_identifier(os.environ["AGENT_AUDIT_TABLE"])
AGENT_TIMEOUT_SECONDS = int(os.getenv("ORDEROPS_AGENT_TIMEOUT_SECONDS", "60"))
MLFLOW_TRACING_ENABLED = os.getenv("ORDEROPS_ENABLE_MLFLOW_TRACING", "true").lower() in (
    "1",
    "true",
    "yes",
)

# -----------------------------------------------------------------------------
# Agent contract
# -----------------------------------------------------------------------------
# The model can reason and choose tools, but the system prompt and the MCP server
# jointly constrain the assistant to read-only, non-PII operational answers.
SYSTEM_PROMPT = """
You are the OrderOps Control Tower assistant, a read-only order operations copilot.

Use the available MCP tools whenever the answer depends on order data. Never invent
an order status, metric, timestamp, or explanation. If a tool returns no data, say
that the information is unavailable. Do not request or reveal names, email addresses,
phone numbers, payment data, or other personal information. Never claim that you
changed, cancelled, inserted, updated, or deleted an order because no write tool is
available. Keep answers concise and mention the relevant order ID, region, status,
or data timestamp when available.
""".strip()

logger = logging.getLogger(__name__)


@st.cache_resource(show_spinner=False)
def get_runtime_resources():
    """Create process-wide clients and workers once, not on every UI rerun.

    In a deployed App, ``WorkspaceClient`` automatically uses the App service
    principal. One agent worker preserves question order. A separate audit
    worker prevents a slow SQL warehouse from delaying the visible answer.
    """
    return (
        WorkspaceClient(),
        ThreadPoolExecutor(max_workers=1, thread_name_prefix="orderops-agent"),
        ThreadPoolExecutor(max_workers=1, thread_name_prefix="orderops-audit"),
    )


workspace, agent_executor, audit_executor = get_runtime_resources()


@st.cache_resource(show_spinner=False)
def configure_tracing():
    """Configure MLflow autologging only once for this App process.

    LangGraph steps, model calls, and tool calls then appear in the bound
    experiment as one connected trace for troubleshooting and evaluation.
    Async trace export keeps a storage retry out of the response path.
    """
    if not MLFLOW_TRACING_ENABLED:
        return False

    mlflow.set_tracking_uri("databricks")
    if MLFLOW_EXPERIMENT_ID:
        mlflow.set_experiment(experiment_id=MLFLOW_EXPERIMENT_ID)
    mlflow.langchain.autolog()
    return True


def get_mcp_url():
    """Resolve the MCP App name to its current Streamable HTTP endpoint.

    ``ORDEROPS_MCP_URL`` is a local-development escape hatch. Deployed Apps use
    the governed App resource name and discover its URL through the SDK.
    """
    local_override = os.getenv("ORDEROPS_MCP_URL")
    if local_override:
        return f"{local_override.rstrip('/')}/mcp"

    app_details = workspace.apps.get(name=MCP_APP_NAME)
    return f"{app_details.url.rstrip('/')}/mcp"


def parse_mcp_result(result):
    """Normalize typed and text MCP responses into one dictionary shape."""
    # SDK/protocol versions may expose structured output with either naming style.
    structured = getattr(result, "structured_content", None) or getattr(
        result, "structuredContent", None
    )
    if structured:
        return structured

    # Fall back to JSON text blocks for servers that do not return typed content.
    text_parts = [block.text for block in result.content if hasattr(block, "text")]
    combined = "".join(text_parts)
    return json.loads(combined) if combined else {}


def call_mcp_tool(tool_name, arguments):
    """Call one MCP tool directly for the deterministic dashboard pages."""
    client = DatabricksMCPClient(
        server_url=get_mcp_url(),
        workspace_client=workspace,
    )
    return parse_mcp_result(client.call_tool(tool_name, arguments))


def run_sql(statement):
    """Execute a fixed-shape statement through the bound SQL warehouse."""
    response = workspace.statement_execution.execute_statement(
        warehouse_id=DATABRICKS_WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    if "SUCCEEDED" not in str(response.status.state):
        raise RuntimeError(f"Audit insert failed: {response.status.state}")


def write_audit(request_id, category, tool_names, outcome, latency_ms):
    """Store low-cardinality request metadata without prompt or response text."""
    # Escape the only string values interpolated into the fixed INSERT template.
    safe_tools = ",".join(tool_names)[:250].replace("'", "''")
    safe_category = category[:50].replace("'", "''")
    safe_outcome = outcome[:30].replace("'", "''")
    run_sql(
        f"""
        INSERT INTO {AGENT_AUDIT_TABLE}
        VALUES (
          '{request_id}',
          current_timestamp(),
          '{safe_category}',
          '{safe_tools}',
          '{safe_outcome}',
          {int(latency_ms)}
        )
        """
    )


def safely_write_audit(request_id, category, tool_names, outcome, latency_ms):
    """Write one audit row and log failures without changing the agent answer."""
    try:
        write_audit(request_id, category, tool_names, outcome, latency_ms)
    except Exception as exc:
        logger.warning("Agent audit write failed for request %s: %s", request_id, exc)


def queue_audit(request_id, category, tool_names, outcome, latency_ms):
    """Submit metadata-only auditing after the user answer is ready."""
    audit_executor.submit(
        safely_write_audit,
        request_id,
        category,
        tool_names,
        outcome,
        latency_ms,
    )


def message_text(message):
    """Return readable text from string or multimodal LangChain content."""
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("text"):
                parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts)
    return str(content)


async def invoke_langgraph(conversation):
    """Discover governed tools, build the agent graph, and run one conversation."""
    # The MCP client supplies OAuth credentials through the WorkspaceClient.
    mcp_server = DatabricksMCPServer(
        name="orderops_mcp",
        url=get_mcp_url(),
        workspace_client=workspace,
    )
    mcp_client = DatabricksMultiServerMCPClient([mcp_server])

    # Current langchain-mcp-adapters versions manage sessions inside get_tools().
    # The client is intentionally not used as an async context manager.
    tools = await mcp_client.get_tools()
    # Temperature zero keeps operational answers as repeatable as possible.
    model = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)
    graph = create_agent(model, tools=tools, system_prompt=SYSTEM_PROMPT)
    result = await graph.ainvoke(
        {"messages": conversation},
        config={"recursion_limit": 8},
    )

    # Read tool messages from graph state for the metadata-only audit record.
    used_tools = []
    for message in result["messages"]:
        name = getattr(message, "name", None)
        if getattr(message, "type", "") == "tool" and name:
            used_tools.append(name)

    return message_text(result["messages"][-1]), sorted(set(used_tools))


def ask_copilot(question, conversation):
    """Apply deterministic input/output guardrails around one agent execution."""
    request_id = str(uuid.uuid4())
    started = time.perf_counter()
    allowed, reason = check_input(question)
    technical_detail = ""

    # Block known unsafe requests before any model or MCP call consumes resources.
    if not allowed:
        elapsed = round((time.perf_counter() - started) * 1000)
        queue_audit(
            request_id,
            classify_question(question),
            [],
            "blocked",
            elapsed,
        )
        return reason, request_id, "", technical_detail

    try:
        # Streamlit calls this synchronous function, so it owns the async lifecycle.
        answer, used_tools = asyncio.run(
            asyncio.wait_for(
                invoke_langgraph(conversation),
                timeout=AGENT_TIMEOUT_SECONDS,
            )
        )
        # Mask accidental PII even when an upstream source changes unexpectedly.
        answer = mask_sensitive_text(answer)
        outcome = "success"
    except TimeoutError:
        answer = (
            "The copilot request timed out. Try a narrower question, then check the "
            "MCP App, model endpoint, and MLflow trace for the request ID."
        )
        used_tools = []
        outcome = "timeout"
        technical_detail = (
            f"Agent request timed out after {AGENT_TIMEOUT_SECONDS} seconds."
        )
    except Exception as exc:
        answer = "The copilot could not complete the request. Check the application logs and resource permissions."
        used_tools = []
        outcome = "error"
        technical_detail = str(exc)

    elapsed = round((time.perf_counter() - started) * 1000)
    # Audit success and failure uniformly without retaining conversational content.
    queue_audit(
        request_id,
        classify_question(question),
        used_tools,
        outcome,
        elapsed,
    )
    return answer, request_id, "", technical_detail


def start_copilot_job(question):
    """Queue one ReAct request without blocking the Streamlit render cycle."""
    conversation = [{"role": "user", "content": question}]
    return agent_executor.submit(ask_copilot, question, conversation)


def find_message_by_job_id(job_id):
    """Return the assistant placeholder linked to a queued ReAct job."""
    for item in st.session_state.messages:
        if item.get("job_id") == job_id:
            return item
    return None


def start_next_agent_job():
    """Start the next queued ReAct job when no other job is active."""
    if st.session_state.active_job_id or not st.session_state.agent_queue:
        return False

    job_id = st.session_state.agent_queue.pop(0)
    item = find_message_by_job_id(job_id)
    if not item:
        return False

    item["status"] = "running"
    item["started_at"] = time.time()
    st.session_state.active_job_id = job_id
    st.session_state.agent_jobs[job_id] = start_copilot_job(item["question"])
    return True


def collect_finished_agent_job():
    """Copy a finished background result into its assistant message."""
    job_id = st.session_state.active_job_id
    if not job_id:
        return False

    future = st.session_state.agent_jobs.get(job_id)
    if not future or not future.done():
        return False

    item = find_message_by_job_id(job_id)
    if not item:
        st.session_state.agent_jobs.pop(job_id, None)
        st.session_state.active_job_id = ""
        return True

    try:
        answer, request_id, audit_warning, technical_detail = future.result()
    except Exception as exc:
        answer = "The copilot could not complete the request. Check the application logs."
        request_id = item.get("request_id") or str(uuid.uuid4())
        audit_warning = ""
        technical_detail = str(exc)

    item.update(
        {
            "content": answer,
            "request_id": request_id,
            "audit_warning": audit_warning,
            "technical_detail": technical_detail,
            "status": "done",
        }
    )
    st.session_state.agent_jobs.pop(job_id, None)
    st.session_state.active_job_id = ""
    return True


def update_agent_jobs():
    """Advance queued and running ReAct jobs without blocking page rendering."""
    changed = collect_finished_agent_job()
    changed = start_next_agent_job() or changed
    return changed


def render_chat_message(item):
    """Render normal messages, queued placeholders, and running placeholders."""
    with st.chat_message(item["role"]):
        status = item.get("status")
        if status == "queued":
            st.info("Queued for the ReAct agent.")
        elif status == "running":
            elapsed = int(time.time() - item.get("started_at", time.time()))
            st.info(f"ReAct agent is working... {elapsed}s elapsed")
            st.caption(
                "Model reasoning, MCP tool call, SQL execution, and trace export may take a moment."
            )
        else:
            if item.get("technical_detail"):
                st.error(f"Technical detail: {item['technical_detail']}")
            if item.get("audit_warning"):
                st.warning(item["audit_warning"])
            st.markdown(item["content"])
            if item.get("request_id"):
                st.caption(f"Request ID: {item['request_id']}")


def initialize_copilot_state():
    """Create the session keys used by the non-blocking Copilot queue."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent_jobs" not in st.session_state:
        st.session_state.agent_jobs = {}
    if "agent_queue" not in st.session_state:
        st.session_state.agent_queue = []
    if "active_job_id" not in st.session_state:
        st.session_state.active_job_id = ""


def enqueue_copilot_question(question):
    """Add a visible user message and an assistant placeholder for one question."""
    job_id = str(uuid.uuid4())
    st.session_state.messages.append({"role": "user", "content": question})
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": "",
            "status": "queued",
            "job_id": job_id,
            "question": question,
            "request_id": "",
            "audit_warning": "",
            "technical_detail": "",
        }
    )
    st.session_state.agent_queue.append(job_id)


@st.fragment(run_every="2s")
def render_copilot_chat():
    """Render and refresh only the chat region while ReAct work is running.

    The chat input deliberately lives inside this fragment. Submitting a
    question therefore reruns only this region, not the full Streamlit page.
    The two-second timer only checks ``Future.done()`` and redraws this region;
    model, MCP, SQL, and trace work remain in background threads.
    """
    initialize_copilot_state()
    update_agent_jobs()

    for item in st.session_state.messages:
        render_chat_message(item)

    active_or_queued = bool(
        st.session_state.active_job_id or st.session_state.agent_queue
    )
    if active_or_queued:
        st.caption(
            "The ReAct agent is working in the background. You can submit another "
            "OrderOps question now; it will be answered in order."
        )

    question = st.chat_input(
        "Ask about orders, changes, exceptions, or regional metrics",
        key="orderops_copilot_question",
    )
    if question:
        enqueue_copilot_question(question)
        update_agent_jobs()
        # The fragment rendered before it received the new question. A
        # fragment-only rerun immediately paints the message and work status.
        st.rerun(scope="fragment")


# -----------------------------------------------------------------------------
# Streamlit user interface
# -----------------------------------------------------------------------------
# Configure tracing before the first agent request so MLflow can capture spans.
configure_tracing()

st.title("📦 OrderOps Control Tower")
st.caption("CDC-powered order operations with governed MCP tools and a LangGraph copilot")

page = st.sidebar.radio("Page", ["Operations", "Order timeline", "Copilot"])
st.sidebar.caption(f"Model endpoint: {LLM_ENDPOINT}")

# Dashboard page: direct MCP calls make the metrics predictable and inexpensive.
if page == "Operations":
    st.subheader("Current operations")
    try:
        summary = call_mcp_tool("get_operations_summary", {"hours": 24})
        metrics = pd.DataFrame(summary.get("regional_metrics", []))
        changes = pd.DataFrame(summary.get("recent_changes", []))

        if not metrics.empty:
            # SQL values may arrive as strings; normalize them before aggregation.
            metrics["order_count"] = pd.to_numeric(metrics["order_count"])
            metrics["delayed_count"] = pd.to_numeric(metrics["delayed_count"])
            metrics["total_amount"] = pd.to_numeric(metrics["total_amount"])
            total_orders = int(metrics["order_count"].sum())
            delayed_orders = int(metrics["delayed_count"].sum())
            total_value = float(metrics["total_amount"].sum())

            first, second, third = st.columns(3)
            first.metric("Current orders", f"{total_orders:,}")
            second.metric("Delayed orders", f"{delayed_orders:,}")
            third.metric("Order value", f"${total_value:,.2f}")
            st.dataframe(metrics, use_container_width=True, hide_index=True)
        else:
            st.info("No metrics are available. Run the data notebooks first.")

        st.subheader("Changes in the last 24 hours")
        st.dataframe(changes, use_container_width=True, hide_index=True)

        st.subheader("Open exceptions")
        exceptions = call_mcp_tool(
            "list_order_exceptions", {"region": "ALL", "limit": 20}
        )
        st.dataframe(
            pd.DataFrame(exceptions.get("exceptions", [])),
            use_container_width=True,
            hide_index=True,
        )
    except Exception as exc:
        st.error("The MCP server could not be reached or queried.")
        st.exception(exc)

# Timeline page: combine one current snapshot with its ordered CDF history.
elif page == "Order timeline":
    st.subheader("Order change history")
    order_id = st.text_input("Order ID", value="ORD-00001").strip().upper()
    if st.button("Load timeline", type="primary"):
        try:
            snapshot = call_mcp_tool("get_order_snapshot", {"order_id": order_id})
            timeline = call_mcp_tool("get_order_timeline", {"order_id": order_id})
            if snapshot.get("found"):
                st.json(snapshot["order"])
            else:
                st.warning("The order is not present in the current-state table.")
            st.dataframe(
                pd.DataFrame(timeline.get("changes", [])),
                use_container_width=True,
                hide_index=True,
            )
        except Exception as exc:
            st.error(str(exc))

# Copilot page: LangGraph decides which read-only MCP tools answer the question.
else:
    st.subheader("Ask the operations copilot")
    st.caption(
        "Try: “What happened to ORD-00001?” or “Which region has the most delayed orders?”"
    )

    # Messages, polling, and input share one fragment. Question submission and
    # result polling therefore never rerun or fade the complete application.
    render_copilot_chat()
