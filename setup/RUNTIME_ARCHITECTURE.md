# OrderOps Control Tower runtime architecture explained

This guide explains what each runtime component does, where it lives in the codebase, and how one user question travels through the whole system.

The main setup guide tells you how to deploy the project. This file explains why the deployed pieces exist and how they interact.

## 1. The simple mental model

The project has two Databricks Apps:

| Zone | App | Main responsibility |
|---|---|---|
| Zone A | MCP App | Controlled, read-only access to Delta tables |
| Zone B | Streamlit and LangGraph App | User interface, agent reasoning, model call, tracing, and audit |

The two Apps are intentionally separated.

Zone A owns data access. Zone B owns user interaction and AI reasoning. In the Copilot page, the Streamlit UI queues valid ReAct requests in a background worker so the page can show progress while LangGraph, the model endpoint, MCP, SQL, and MLflow do the slower work.

```text
User
  |
  v
Zone B App: Streamlit + LangGraph Copilot
  |
  | calls model endpoint
  v
Databricks Model Serving / Unity AI Gateway
  |
  v
Foundation Model API

Zone B App also calls:
  |
  v
Zone A App: MCP Server
  |
  v
SQL Warehouse
  |
  v
Unity Catalog Delta tables
```

## 2. What Zone A App is

Zone A is the MCP server App.

Code location:

```text
apps/mcp_server/
```

Important files:

| File | Purpose |
|---|---|
| `apps/mcp_server/app.py` | Starts the FastMCP server and exposes the project tools |
| `apps/mcp_server/validation.py` | Validates order IDs, regions, limits, and table names |
| `apps/mcp_server/app.yaml` | Maps Databricks App resources into environment variables |
| `apps/mcp_server/requirements.txt` | Runtime dependencies for the MCP App |

Zone A exposes four read-only tools:

| MCP tool | What it returns |
|---|---|
| `get_order_snapshot` | Current state for one order |
| `get_order_timeline` | CDC-derived history for one order |
| `list_order_exceptions` | Delayed or overdue orders |
| `get_operations_summary` | Current metrics and recent changes |

Zone A directly reads these Delta tables:

```text
orderops.silver.orders_current
orderops.gold.order_history
orderops.gold.order_metrics
```

Zone A does not call an LLM. It does not have a chat UI. It does not expose arbitrary SQL. It only exposes fixed business tools.

This is the key protection:

```text
The agent cannot directly query operational tables.
The agent can only ask Zone A to run approved tools.
```

## 3. What Zone B App is

Zone B is the user-facing Streamlit and LangGraph App.

Code location:

```text
apps/copilot_app/
```

Important files:

| File | Purpose |
|---|---|
| `apps/copilot_app/app.py` | Streamlit UI, LangGraph agent, MCP client calls, audit write, and MLflow setup |
| `apps/copilot_app/guardrails.py` | Input blocking, output masking, request classification, and SQL identifier safety |
| `apps/copilot_app/app.yaml` | Maps Databricks App resources into environment variables |
| `apps/copilot_app/requirements.txt` | Runtime dependencies for the Copilot App |

Zone B has three pages:

| Page | How it works |
|---|---|
| Operations | Calls the MCP App directly and shows metrics |
| Order timeline | Calls the MCP App directly for one order |
| Copilot | Uses LangGraph, the LLM endpoint, and MCP tools for allowed OrderOps questions |

Zone B is allowed to call:

```text
MCP App
LLM serving endpoint
MLflow experiment
SQL warehouse for audit insert
monitoring.agent_audit table for metadata writes
```

Zone B is not granted direct `SELECT` access on the operational silver and gold tables. This keeps the agent path honest: data-dependent answers must flow through MCP tools.

## 4. What MCP means in this project

MCP means Model Context Protocol.

In this project, MCP is used as a clean tool boundary between the agent and the data platform.

Instead of giving the agent raw table access, the MCP server gives it a small menu of safe operations:

```text
get_order_snapshot(order_id)
get_order_timeline(order_id)
list_order_exceptions(region, limit)
get_operations_summary(hours)
```

That gives three benefits:

1. The agent gets useful business context.
2. The data platform stays protected from arbitrary SQL.
3. Tool inputs and outputs are easier to validate, test, and explain.

## 5. What ReAct means here

In this project, ReAct does not mean React JS.

ReAct means:

```text
Reason + Act
```

The LangGraph ReAct agent can reason about a question, choose a tool, observe the result, and then answer.

The agent is created in:

```text
apps/copilot_app/app.py
```

The important flow is:

```python
tools = await mcp_client.get_tools()
model = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)
graph = create_agent(model, tools=tools, system_prompt=SYSTEM_PROMPT)
result = await graph.ainvoke(
    {"messages": conversation},
    config={"recursion_limit": 8},
)
```

The ReAct loop looks like this:

```text
1. User asks a question.
2. The agent reasons about what information is needed.
3. The agent decides whether a tool is required.
4. The agent calls an MCP tool if data is needed.
5. The MCP tool returns structured data.
6. The agent reasons over the result.
7. The agent produces the final answer.
```

Example:

```text
Question:
What happened to ORD-00001?

Agent decision:
I need order history.

Tool call:
get_order_timeline(order_id="ORD-00001")

Final answer:
The order was created, later updated, and is currently in its latest status based on the CDC timeline.
```

## 6. Where Unity AI Gateway comes into the picture

Unity AI Gateway comes into the picture at the model endpoint boundary.

The project does not call an external model directly from Python. The Copilot App calls a Databricks model serving endpoint:

```python
model = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)
```

The endpoint name is configured through the Bundle:

```text
databricks.yml
  -> resources/orderops_apps.yml
  -> apps/copilot_app/app.yaml
  -> DATABRICKS_LLM_ENDPOINT
  -> ChatDatabricks(endpoint=LLM_ENDPOINT)
```

So the actual runtime path is:

```text
LangGraph agent
  -> ChatDatabricks
  -> Databricks model serving endpoint
  -> Unity AI Gateway controls, when available
  -> Foundation Model API
```

Unity AI Gateway can provide endpoint-level governance such as:

| Capability | What it means |
|---|---|
| Usage tracking | Record model endpoint usage, requester, latency, token usage, and errors |
| Rate limits | Limit how frequently users or service principals can call the endpoint |
| Safety policies | Apply platform-level safety filtering where supported |
| PII policies | Block or mask sensitive information where supported |
| Prompt-injection policies | Add platform-level protection where supported |
| Inference logging | Optionally log model request and response payloads when explicitly enabled |

Important distinction:

```text
Unity AI Gateway traces model endpoint traffic.
MLflow traces the full LangGraph agent execution.
```

That means Gateway can help answer:

```text
Who called the model endpoint?
How often was the endpoint called?
How much latency or token usage happened?
Was a Gateway policy triggered?
```

MLflow can help answer:

```text
What did the agent do step by step?
Which MCP tool was called?
What did the tool return?
Where did the final answer come from?
```

## 7. How to configure Unity AI Gateway for this project

After the Bundle deploys the Apps, configure the selected model endpoint from the Databricks UI if the workspace exposes AI Gateway settings.

1. Open **Serving**.
2. Open the endpoint selected for `llm_endpoint_name`.
3. Open the **AI Gateway** section.
4. Select **Edit AI Gateway**.
5. Enable safety controls if available.
6. Configure PII input behavior as **Block** if available.
7. Configure PII output behavior as **Mask** if available.
8. Add a conservative rate limit, such as 10 requests per minute.
9. Enable usage tracking if available.
10. Save and test one normal prompt and one PII-containing prompt.

Free Edition workspaces may not expose every AI Gateway or Unity Gateway policy option. When platform policies are unavailable, the project still has application-level guardrails in:

```text
apps/copilot_app/guardrails.py
apps/mcp_server/validation.py
apps/mcp_server/app.py
resources/orderops_apps.yml
```

Avoid enabling full inference payload logging unless a data retention policy has been decided. For this project, MLflow traces and the `monitoring.agent_audit` table are usually enough for learning and debugging.

The Copilot also blocks off-topic prompts before model inference. For example, `Write Python code to add two numbers` is outside the OrderOps scope and should receive an immediate scope message instead of calling the LLM.

## 8. Why AI Gateway token usage can show zero

The Copilot can answer a question even when the Unity AI Gateway page still shows `0` tokens for a model.

The most common reasons are:

| Reason | What to check |
|---|---|
| Different service name | The App uses `databricks-meta-llama-3-1-8b-instruct`, while the AI Gateway page can show Unity Catalog model services such as `system.ai.meta-llama-3-1-8b-instruct` or the shorter UI name `meta-llama-3-1-8b-instruct` |
| Usage view delay | Gateway usage is aggregated and may not appear immediately after a request |
| Usage tracking scope | `system.ai_gateway.usage` is for Unity Gateway model services; legacy model-serving endpoint traffic can appear under `system.serving.endpoint_usage` instead |
| Permission visibility | Viewing the usage system table can require elevated admin permissions |
| No model call happened | A blocked guardrail request, failed request, or direct dashboard page call may not call the LLM |

For this project, the strongest proof that the LLM path was used is MLflow, not the summary token number on the AI Gateway list page.

Check these places in order:

```text
1. Copilot sidebar shows the endpoint from DATABRICKS_LLM_ENDPOINT.
2. MLflow trace shows a model span for the Copilot request.
3. monitoring.agent_audit shows outcome='success' for the displayed request ID.
4. If available, `system.ai_gateway.usage` or `system.serving.endpoint_usage` shows usage for the matching endpoint name.
```

Example audit check:

```sql
SELECT request_id, event_time, question_category, tool_name, outcome, latency_ms
FROM orderops.monitoring.agent_audit
ORDER BY event_time DESC;
```

Example Unity Gateway usage check, if the workspace grants access:

```sql
SELECT endpoint_name, requester, event_time, input_tokens, output_tokens, total_tokens
FROM system.ai_gateway.usage
WHERE event_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

Example legacy serving-endpoint usage check:

```sql
SELECT endpoint_name, requester, request_time, input_token_count, output_token_count
FROM system.serving.endpoint_usage
WHERE request_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY request_time DESC;
```

If the usage table remains empty but MLflow has a model span, the project is still calling the LLM. It only means the particular Gateway usage view being inspected is not recording or displaying that call path.

## 9. What MLflow does in this project

MLflow captures the agent trace.

It is configured in:

```text
apps/copilot_app/app.py
```

The important setup is:

```python
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(experiment_id=MLFLOW_EXPERIMENT_ID)
mlflow.langchain.autolog()
```

The MLflow experiment is created by the Bundle in:

```text
resources/orderops_apps.yml
```

The experiment name is user-scoped:

```text
/Users/<current-user>/orderops-control-tower-dev-traces
```

MLflow records the agent run as a trace. A successful Copilot request should show spans for:

```text
LangGraph execution
model call
MCP tool call
tool response
final answer
```

Use MLflow when you want to explain or debug the agent's behavior.

Example questions MLflow helps answer:

```text
Did the agent call a tool?
Which tool did it call?
Did the tool fail?
Did the model answer without enough data?
How long did each step take?
```

## 10. What the audit table does

The project also writes one small audit row per Copilot request.

Table:

```text
orderops.monitoring.agent_audit
```

The audit write happens in:

```text
apps/copilot_app/app.py
```

The table stores metadata such as:

```text
request_id
request timestamp
question category
tools used
outcome
latency
```

It does not store prompt text or response text.

This makes it safer for a learning project because the table gives useful operational evidence without becoming a sensitive conversation log.

Use the audit table when you want a simple SQL-friendly view of agent usage:

```sql
SELECT *
FROM orderops.monitoring.agent_audit
ORDER BY request_timestamp DESC;
```

## 11. End-to-end Copilot request flow

This is the full runtime path when a user asks a data-dependent question in the Copilot page.

```text
1. User opens the Zone B Streamlit App.
2. User asks a question.
3. Zone B runs deterministic input guardrails.
4. If the question is blocked, no model call happens.
5. If the question is allowed, Zone B starts LangGraph.
6. LangGraph discovers tools from the Zone A MCP App.
7. LangGraph calls the Databricks model endpoint.
8. Unity AI Gateway governs the model call when endpoint policies are enabled.
9. The model decides whether an MCP tool is needed.
10. Zone B calls the selected MCP tool on Zone A.
11. Zone A validates the tool arguments.
12. Zone A executes fixed read-only SQL through the SQL warehouse.
13. SQL reads Delta tables from Unity Catalog.
14. Zone A returns structured JSON to Zone B.
15. LangGraph sends the tool result back to the model.
16. The model produces a final answer.
17. Zone B runs output masking.
18. MLflow records the LangGraph, model, and tool trace.
19. Zone B writes minimal request metadata into `monitoring.agent_audit`.
20. Streamlit displays the answer and request ID to the user.
```

## 12. End-to-end dashboard page flow

The Operations and Order timeline pages are more deterministic than the Copilot page.

They do not need the LLM.

```text
1. User opens the Zone B Streamlit App.
2. User selects Operations or Order timeline.
3. Zone B directly calls one MCP tool.
4. Zone A validates the request.
5. Zone A reads Delta tables through the SQL warehouse.
6. Zone A returns structured JSON.
7. Streamlit renders the table or metrics.
```

This is useful because it proves the MCP App works before the LLM or LangGraph is involved.

## 13. CDC data flow that feeds the Apps

The Apps only become useful after the Lakeflow Job creates data.

The Job is defined in:

```text
resources/orderops_cdc.job.yml
```

The Job sequence is:

```text
generate_source_events
  -> apply_events_to_current
  -> publish_order_history
  -> refresh_order_metrics
  -> validate_pipeline
```

The tables are:

| Layer | Table | Purpose |
|---|---|---|
| Bronze | `order_events` | Raw synthetic source events |
| Silver | `orders_current` | Latest masked order state, with Change Data Feed enabled |
| Gold | `order_history` | Business history created from Delta CDF |
| Gold | `order_metrics` | Current aggregate for dashboards and tools |
| Monitoring | `agent_audit` | Agent request metadata |

The agent does not create CDC data. The Job creates and updates the data. The Apps read the outputs.

## 14. What to inspect in Databricks UI

Use these UI locations to understand each runtime layer.

| What to inspect | Databricks UI location |
|---|---|
| CDC workflow runs | Jobs & Pipelines |
| Delta tables and table permissions | Catalog Explorer |
| MCP App status and logs | Compute > Apps |
| Streamlit Copilot App status and logs | Compute > Apps |
| Model endpoint and AI Gateway settings | Serving |
| Model playground behavior | AI/ML > Playground |
| LangGraph traces | Experiments > orderops-control-tower-dev-traces > Traces |
| Request metadata | SQL editor query against `monitoring.agent_audit` |

## 15. How the main pieces differ

| Component | What it is | What it controls |
|---|---|---|
| Delta Lake CDC | Data engineering pattern | Current state and history from inserts, updates, and deletes |
| Lakeflow Job | Orchestration layer | Notebook execution order and failure behavior |
| Unity Catalog | Governance catalog | Tables, schemas, permissions, and securable objects |
| Zone A MCP App | Tool service | Safe, read-only business tools |
| Zone B Streamlit App | User-facing app | Dashboard pages and Copilot page |
| LangGraph | Agent orchestration | ReAct reasoning, model calls, and tool calls |
| Databricks model endpoint | Model serving layer | Hosted model access |
| Unity AI Gateway | Model governance layer | Endpoint policies, usage tracking, rate limits, and safety controls |
| MLflow | Observability layer | Detailed agent traces |
| `agent_audit` table | Lightweight audit layer | SQL-friendly request metadata |

## 16. High level flow

```text
OrderOps Control Tower separates data access from agent reasoning.

The Lakeflow Job maintains Delta CDC tables. The MCP App exposes a small set of
approved read-only tools over those tables. The Streamlit and LangGraph App uses
those tools to answer operational questions.

When the LangGraph agent needs reasoning, it calls a Databricks model serving
endpoint through ChatDatabricks. Unity AI Gateway can govern that endpoint with
usage tracking, rate limits, safety policies, and PII controls. MLflow captures
the complete LangGraph trace, while the audit table stores only minimal request
metadata.
```

That is the core design of the project.
