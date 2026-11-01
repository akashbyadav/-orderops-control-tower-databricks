"""OrderOps Control Tower MCP server.

The server exposes four read-only, business-focused tools. It intentionally does
not expose arbitrary SQL or data-mutation capabilities.
"""

import os
from datetime import datetime, timezone

import uvicorn
from databricks.sdk import WorkspaceClient
from fastmcp import FastMCP

from validation import clamp_hours, clamp_limit, quote_table_name, validate_order_id, validate_region

# -----------------------------------------------------------------------------
# Governed runtime configuration
# -----------------------------------------------------------------------------
# app.yaml injects these values from bundle resources. Table identifiers are
# validated and quoted once before they are ever placed in an SQL template.
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
ORDERS_CURRENT = quote_table_name(os.environ["ORDERS_CURRENT_TABLE"])
ORDER_HISTORY = quote_table_name(os.environ["ORDER_HISTORY_TABLE"])
ORDER_METRICS = quote_table_name(os.environ["ORDER_METRICS_TABLE"])

# WorkspaceClient uses the MCP App service principal in Databricks Apps. FastMCP
# publishes the small business-tool contract consumed by the UI and LangGraph.
workspace = WorkspaceClient()
mcp_server = FastMCP("OrderOps MCP")


def query_rows(statement):
    """Run one bounded SQL query and return JSON-serializable dictionaries."""
    response = workspace.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )

    state = str(response.status.state)
    if "SUCCEEDED" not in state:
        message = getattr(response.status, "error", None) or state
        raise RuntimeError(f"Databricks SQL statement failed: {message}")

    # Statement Execution returns column metadata separately from row arrays.
    columns = [column.name for column in response.manifest.schema.columns]
    data = response.result.data_array or []
    return [dict(zip(columns, row)) for row in data]


@mcp_server.tool()
def get_order_snapshot(order_id: str) -> dict:
    """Return the current non-PII operational state for one order ID."""
    # Validation makes interpolation safe and produces a useful MCP error message.
    order_id = validate_order_id(order_id)
    rows = query_rows(
        f"""
        SELECT
          order_id,
          region,
          product_category,
          order_amount,
          order_status,
          order_created_at,
          promised_delivery_at,
          actual_delivery_at,
          updated_at,
          CASE
            WHEN order_status = 'DELAYED'
              OR (
                promised_delivery_at < current_timestamp()
                AND order_status NOT IN ('DELIVERED', 'CANCELLED')
              )
            THEN true ELSE false
          END AS is_delayed
        FROM {ORDERS_CURRENT}
        WHERE order_id = '{order_id}'
        LIMIT 1
        """
    )
    return {"found": bool(rows), "order": rows[0] if rows else None}


@mcp_server.tool()
def get_order_timeline(order_id: str) -> dict:
    """Return the ordered CDC history for one order ID, newest change first."""
    order_id = validate_order_id(order_id)
    rows = query_rows(
        f"""
        SELECT
          order_id,
          change_type,
          commit_version,
          commit_timestamp,
          old_status,
          new_status,
          region,
          order_amount
        FROM {ORDER_HISTORY}
        WHERE order_id = '{order_id}'
        ORDER BY commit_version DESC
        LIMIT 25
        """
    )
    return {"order_id": order_id, "changes": rows, "change_count": len(rows)}


@mcp_server.tool()
def list_order_exceptions(region: str = "ALL", limit: int = 20) -> dict:
    """List delayed or overdue current orders for a region without returning PII."""
    # Region is an allow-listed dimension and limit is clamped before interpolation.
    region = validate_region(region)
    limit = clamp_limit(limit, maximum=50)
    region_filter = "" if region == "ALL" else f"AND region = '{region}'"

    rows = query_rows(
        f"""
        SELECT
          order_id,
          region,
          product_category,
          order_amount,
          order_status,
          promised_delivery_at,
          updated_at,
          GREATEST(
            0,
            TIMESTAMPDIFF(HOUR, promised_delivery_at, current_timestamp())
          ) AS delay_hours
        FROM {ORDERS_CURRENT}
        WHERE (
          order_status = 'DELAYED'
          OR (
            promised_delivery_at < current_timestamp()
            AND order_status NOT IN ('DELIVERED', 'CANCELLED')
          )
        )
        {region_filter}
        ORDER BY delay_hours DESC, order_id
        LIMIT {limit}
        """
    )
    return {"region": region, "exceptions": rows, "exception_count": len(rows)}


@mcp_server.tool()
def get_operations_summary(hours: int = 24) -> dict:
    """Return current regional metrics and CDC change counts for a recent time window."""
    # The tool deliberately combines only the two small datasets needed by the UI.
    hours = clamp_hours(hours)
    metrics = query_rows(
        f"""
        SELECT
          region,
          SUM(order_count) AS order_count,
          SUM(delayed_count) AS delayed_count,
          CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_amount,
          MAX(refreshed_at) AS refreshed_at
        FROM {ORDER_METRICS}
        WHERE metric_date = current_date()
        GROUP BY region
        ORDER BY region
        """
    )
    recent_changes = query_rows(
        f"""
        SELECT change_type, COUNT(*) AS change_count
        FROM {ORDER_HISTORY}
        WHERE commit_timestamp >= TIMESTAMPADD(HOUR, -{hours}, current_timestamp())
        GROUP BY change_type
        ORDER BY change_type
        """
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": hours,
        "regional_metrics": metrics,
        "recent_changes": recent_changes,
    }


# FastMCP exposes the stateless Streamable HTTP endpoint at /mcp. Stateless mode
# fits Databricks Apps scaling because conversation state remains in LangGraph.
app = mcp_server.http_app(stateless_http=True)


if __name__ == "__main__":
    # Databricks Apps injects the runtime port as an environment variable. Reading
    # it here avoids shell-expansion assumptions in app.yaml and gives Uvicorn the
    # integer value it requires.
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
