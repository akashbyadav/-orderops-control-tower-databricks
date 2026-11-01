# Databricks notebook source
"""Read and validate the parameters shared by all project notebooks.

Lakeflow Jobs exposes job parameters as notebook widgets. Interactive notebook
runs do not, so ``read_parameter`` creates the same widget with a safe default.
This keeps one implementation usable from both the Job UI and a notebook.
"""

# COMMAND ----------

import re

# -----------------------------------------------------------------------------
# Widget and identifier helpers
# -----------------------------------------------------------------------------

def read_parameter(name, default, label):
    """Read a Job parameter, creating its widget only for an interactive run."""
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        dbutils.widgets.text(name, str(default), label)
        return dbutils.widgets.get(name).strip()


def quote_identifier(value):
    """Quote a validated single Unity Catalog identifier."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,254}", value):
        raise ValueError(
            "catalog_name must start with a letter and contain only letters, "
            "numbers, and underscores"
        )
    return f"`{value}`"


# -----------------------------------------------------------------------------
# Shared Job parameters
# -----------------------------------------------------------------------------
# Lakeflow supplies these widgets during a Job run. The defaults make an
# interactive run behave exactly like the first sample Job run.
CATALOG = read_parameter("catalog_name", "orderops", "Unity Catalog catalog")
LOAD_MODE = read_parameter("load_mode", "initial", "initial or cdc").lower()
INITIAL_ORDER_COUNT = int(
    read_parameter("initial_order_count", "200", "Initial order count")
)
CDC_BATCH_ID = int(read_parameter("cdc_batch_id", "2", "CDC batch ID"))
CDC_BATCH_SIZE = int(read_parameter("cdc_batch_size", "15", "CDC batch size"))

# Validate all external values once so downstream notebooks can use them safely.
quote_identifier(CATALOG)

if LOAD_MODE not in {"initial", "cdc"}:
    raise ValueError("load_mode must be either initial or cdc")
if not 20 <= INITIAL_ORDER_COUNT <= 5000:
    raise ValueError("initial_order_count must be between 20 and 5000")
if CDC_BATCH_ID < 2:
    raise ValueError("cdc_batch_id must be 2 or greater")
if not 3 <= CDC_BATCH_SIZE <= 100:
    raise ValueError("cdc_batch_size must be between 3 and 100")

# -----------------------------------------------------------------------------
# Fully qualified Unity Catalog object names
# -----------------------------------------------------------------------------
# Centralizing names prevents individual notebooks from drifting across schemas.
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
MONITORING_SCHEMA = f"{CATALOG}.monitoring"

BRONZE_EVENTS = f"{BRONZE_SCHEMA}.order_events"
SILVER_CURRENT = f"{SILVER_SCHEMA}.orders_current"
GOLD_HISTORY = f"{GOLD_SCHEMA}.order_history"
GOLD_METRICS = f"{GOLD_SCHEMA}.order_metrics"
AGENT_AUDIT = f"{MONITORING_SCHEMA}.agent_audit"

# A compact log line makes Job-run parameters visible during troubleshooting.
print(f"catalog={CATALOG}, load_mode={LOAD_MODE}, cdc_batch_id={CDC_BATCH_ID}")
