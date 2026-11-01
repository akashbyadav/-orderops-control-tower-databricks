# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Fail the workflow when CDC correctness or privacy expectations are violated."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def scalar(query, column):
    """Return one named value from a single-row SQL query."""
    return spark.sql(query).first()[column]


# Each tuple is (check name, passed flag, observed detail). This simple shape keeps
# the validation output easy to display and to raise as a Job failure.
checks = []

# -----------------------------------------------------------------------------
# Check group 1: every expected layer has data
# -----------------------------------------------------------------------------
bronze_count = spark.table(BRONZE_EVENTS).count()
silver_count = spark.table(SILVER_CURRENT).count()
history_count = spark.table(GOLD_HISTORY).count()
metric_count = spark.table(GOLD_METRICS).count()

checks.extend(
    [
        ("bronze_has_events", bronze_count > 0, bronze_count),
        ("silver_has_current_orders", silver_count > 0, silver_count),
        ("history_has_cdf_rows", history_count > 0, history_count),
        ("metrics_have_rows", metric_count > 0, metric_count),
    ]
)

# -----------------------------------------------------------------------------
# Check group 2: current-state key and masking invariants
# -----------------------------------------------------------------------------
duplicate_count = scalar(
    f"""
    SELECT COUNT(*) AS count
    FROM (
      SELECT order_id
      FROM {SILVER_CURRENT}
      GROUP BY order_id
      HAVING COUNT(*) > 1
    )
    """,
    "count",
)
unmasked_email_count = scalar(
    f"""
    SELECT COUNT(*) AS count
    FROM {SILVER_CURRENT}
    WHERE customer_email_masked IS NOT NULL
      AND customer_email_masked NOT RLIKE '^[^@]\\*{{3}}@.+$'
    """,
    "count",
)

checks.extend(
    [
        ("current_order_id_is_unique", duplicate_count == 0, duplicate_count),
        ("silver_email_is_masked", unmasked_email_count == 0, unmasked_email_count),
    ]
)

# -----------------------------------------------------------------------------
# Check group 3: cross-layer reconciliation
# -----------------------------------------------------------------------------
# Rebuild the expected current key set independently from the event log. This
# detects missed deletes, stale silver records, and incorrect latest-event logic.
latest_window = Window.partitionBy("order_id").orderBy(
    F.col("event_timestamp").desc(), F.col("event_id").desc()
)
expected_current_count = (
    spark.table(BRONZE_EVENTS)
    .withColumn("row_number", F.row_number().over(latest_window))
    .filter((F.col("row_number") == 1) & (F.col("operation") != "D"))
    .count()
)
metric_order_count = scalar(
    f"""
    SELECT COALESCE(SUM(order_count), 0) AS count
    FROM {GOLD_METRICS}
    WHERE metric_date = current_date()
    """,
    "count",
)

checks.extend(
    [
        (
            "silver_matches_latest_non_delete_events",
            silver_count == expected_current_count,
            f"silver={silver_count}, expected={expected_current_count}",
        ),
        (
            "metrics_reconcile_to_silver",
            metric_order_count == silver_count,
            f"metrics={metric_order_count}, silver={silver_count}",
        ),
    ]
)

# -----------------------------------------------------------------------------
# Publish results and fail the Lakeflow task when any invariant is broken
# -----------------------------------------------------------------------------
failed = [name for name, passed, detail in checks if not passed]
result_rows = [
    {"check_name": name, "passed": passed, "observed": str(detail)}
    for name, passed, detail in checks
]
display(spark.createDataFrame(result_rows).orderBy("check_name"))

# Raising an exception gives the workflow an explicit red failure state.
if failed:
    raise AssertionError("Pipeline quality checks failed: " + ", ".join(failed))

# Expose a compact success value to the Job UI without breaking interactive runs.
try:
    dbutils.jobs.taskValues.set(key="validation_status", value="passed")
except Exception:
    pass

print(f"All {len(checks)} pipeline quality checks passed.")
