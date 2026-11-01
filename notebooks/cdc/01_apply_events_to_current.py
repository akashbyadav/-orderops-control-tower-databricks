# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Apply the latest CDC event for each order to the current-state Delta table."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Step 1: select the newest source event for every order
# -----------------------------------------------------------------------------
# event_id is the deterministic tie-breaker when two events share a timestamp.
latest_event_window = Window.partitionBy("order_id").orderBy(
    F.col("event_timestamp").desc(), F.col("event_id").desc()
)

latest_events = (
    spark.table(BRONZE_EVENTS)
    .withColumn("row_number", F.row_number().over(latest_event_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    # Mask PII before it crosses from the restricted bronze layer into silver.
    .withColumn(
        "customer_name_masked",
        F.when(
            F.col("customer_name").isNotNull(),
            F.concat(F.substring("customer_name", 1, 1), F.lit("***")),
        ),
    )
    .withColumn(
        "customer_email_masked",
        F.when(
            F.col("customer_email").contains("@"),
            F.concat(
                F.substring("customer_email", 1, 1),
                F.lit("***@"),
                F.element_at(F.split("customer_email", "@"), 2),
            ),
        ),
    )
)

latest_events.createOrReplaceTempView("latest_order_events")

# COMMAND ----------

# -----------------------------------------------------------------------------
# Step 2: materialize current state with an idempotent Delta MERGE
# -----------------------------------------------------------------------------
# The timestamp predicate prevents a rerun from producing duplicate updates in
# Change Data Feed. Delete removes current state; update/insert writes masked data.
spark.sql(
    f"""
    MERGE INTO {SILVER_CURRENT} AS target
    USING latest_order_events AS source
      ON target.order_id = source.order_id

    WHEN MATCHED
      AND source.operation = 'D'
      AND source.event_timestamp > target.updated_at
      THEN DELETE

    WHEN MATCHED
      AND source.operation IN ('I', 'U')
      AND source.event_timestamp > target.updated_at
      THEN UPDATE SET
        target.customer_name_masked = source.customer_name_masked,
        target.customer_email_masked = source.customer_email_masked,
        target.region = source.region,
        target.product_category = source.product_category,
        target.order_amount = source.order_amount,
        target.order_status = source.order_status,
        target.order_created_at = source.order_created_at,
        target.promised_delivery_at = source.promised_delivery_at,
        target.actual_delivery_at = source.actual_delivery_at,
        target.updated_at = source.event_timestamp,
        target.source_event_id = source.event_id,
        target.source_batch_id = source.batch_id

    WHEN NOT MATCHED
      AND source.operation IN ('I', 'U')
      THEN INSERT (
        order_id,
        customer_name_masked,
        customer_email_masked,
        region,
        product_category,
        order_amount,
        order_status,
        order_created_at,
        promised_delivery_at,
        actual_delivery_at,
        updated_at,
        source_event_id,
        source_batch_id
      ) VALUES (
        source.order_id,
        source.customer_name_masked,
        source.customer_email_masked,
        source.region,
        source.product_category,
        source.order_amount,
        source.order_status,
        source.order_created_at,
        source.promised_delivery_at,
        source.actual_delivery_at,
        source.event_timestamp,
        source.event_id,
        source.batch_id
      )
    """
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# Step 3: expose a compact run result
# -----------------------------------------------------------------------------
display(
    spark.sql(
        f"""
        SELECT source_batch_id, order_status, COUNT(*) AS order_count
        FROM {SILVER_CURRENT}
        GROUP BY source_batch_id, order_status
        ORDER BY source_batch_id, order_status
        """
    )
)

# Task values help downstream Job troubleshooting but are optional interactively.
try:
    current_count = spark.table(SILVER_CURRENT).count()
    dbutils.jobs.taskValues.set(key="current_order_count", value=current_count)
except Exception:
    pass
