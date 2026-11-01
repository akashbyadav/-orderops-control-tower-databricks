# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Publish the small current-state aggregate consumed by the MCP server."""

# -----------------------------------------------------------------------------
# Step 1: rebuild the serving snapshot
# -----------------------------------------------------------------------------
# The table is a current snapshot, so INSERT OVERWRITE is deliberately clearer
# than maintaining several incremental aggregate branches for this project size.
spark.sql(
    f"""
    INSERT OVERWRITE {GOLD_METRICS}
    SELECT
      current_date() AS metric_date,
      region,
      order_status,
      COUNT(*) AS order_count,
      CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS total_amount,
      SUM(
        CASE
          WHEN order_status = 'DELAYED'
            OR (
              promised_delivery_at < current_timestamp()
              AND order_status NOT IN ('DELIVERED', 'CANCELLED')
            )
          THEN 1 ELSE 0
        END
      ) AS delayed_count,
      current_timestamp() AS refreshed_at
    FROM {SILVER_CURRENT}
    GROUP BY region, order_status
    """
)

# -----------------------------------------------------------------------------
# Step 2: report the refreshed result
# -----------------------------------------------------------------------------
metric_rows = spark.table(GOLD_METRICS).count()
# The task value is useful in Job runs and harmlessly skipped in interactive runs.
try:
    dbutils.jobs.taskValues.set(key="metric_row_count", value=metric_rows)
except Exception:
    pass

# Final preview is ordered for predictable visual comparison between runs.
display(spark.table(GOLD_METRICS).orderBy("region", "order_status"))
