# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Generate either the initial event set or one incremental CDC batch.

Job parameter ``load_mode`` controls the branch:
- ``initial`` creates deterministic batch 1 and is safe to rerun.
- ``cdc`` creates a new batch of inserts, updates, and deletes.

Synthetic data is generated only for demonstration. The downstream CDC tasks do
not depend on how the events were produced, so a real source can replace this task.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Synthetic source configuration
# -----------------------------------------------------------------------------
# Reuse the registered table schema so generated Decimal/Timestamp values always
# match the downstream Delta contract.
BRONZE_SCHEMA_DEFINITION = spark.table(BRONZE_EVENTS).schema
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
CATEGORIES = ["BOOKS", "ELECTRONICS", "HOME", "SPORTS"]


def money(value):
    """Return a two-decimal value compatible with the target Decimal column."""
    return Decimal(f"{value:.2f}")


def initial_events():
    """Build deterministic inserts for batch 1."""
    # A fixed seed makes demonstrations and validation results reproducible.
    random.seed(42)
    first_names = ["Aarav", "Aisha", "Daniel", "Isha", "Kabir", "Maya", "Noah", "Priya"]
    last_names = ["Bose", "Gupta", "Johnson", "Khan", "Lee", "Patel", "Rao", "Shah"]
    statuses = ["PLACED", "PACKED", "SHIPPED", "DELIVERED", "DELAYED"]
    weights = [18, 20, 28, 26, 8]
    base_time = datetime.utcnow().replace(microsecond=0) - timedelta(days=7)
    rows = []

    # Create one insert event per order, including a small share of delayed orders.
    for index in range(1, INITIAL_ORDER_COUNT + 1):
        order_id = f"ORD-{index:05d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        created_at = base_time + timedelta(minutes=index * 31)
        promised_at = created_at + timedelta(days=random.choice([2, 3, 4]))
        status = random.choices(statuses, weights=weights, k=1)[0]
        actual_at = None

        if status == "DELIVERED":
            actual_at = promised_at - timedelta(hours=random.randint(1, 18))
        elif status == "DELAYED":
            promised_at = datetime.utcnow().replace(microsecond=0) - timedelta(
                hours=random.randint(2, 36)
            )

        rows.append(
            {
                "event_id": f"EVT-B001-{index:05d}",
                "batch_id": 1,
                "order_id": order_id,
                "operation": "I",
                "customer_name": f"{first_name} {last_name}",
                "customer_email": f"{first_name.lower()}.{last_name.lower()}{index}@example.com",
                "region": random.choice(REGIONS),
                "product_category": random.choice(CATEGORIES),
                "order_amount": money(random.uniform(15, 750)),
                "order_status": status,
                "order_created_at": created_at,
                "promised_delivery_at": promised_at,
                "actual_delivery_at": actual_at,
                "event_timestamp": created_at,
            }
        )

    return rows, 1


def incremental_events():
    """Build a new batch from the latest attributes of current orders."""
    # Derive the seed from batch ID: a given batch is predictable but each ID differs.
    random.seed(1000 + CDC_BATCH_ID)

    # A batch ID is a simple ingestion idempotency key for this generated source.
    existing = spark.sql(
        f"SELECT COUNT(*) AS count FROM {BRONZE_EVENTS} "
        f"WHERE batch_id = {CDC_BATCH_ID}"
    ).first()["count"]
    if existing:
        raise ValueError(
            f"Batch {CDC_BATCH_ID} already exists. Use a new cdc_batch_id."
        )

    # Updates/deletes require the initial workflow to have materialized silver state.
    if spark.table(SILVER_CURRENT).limit(1).count() == 0:
        raise ValueError("Run the workflow once with load_mode=initial before cdc mode.")

    # Reconstruct each surviving order's newest full payload from the bronze log.
    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("event_timestamp").desc(), F.col("event_id").desc()
    )
    current_ids = {
        row["order_id"]
        for row in spark.table(SILVER_CURRENT).select("order_id").collect()
    }
    source_rows = [
        row.asDict()
        for row in (
            spark.table(BRONZE_EVENTS)
            .withColumn("row_number", F.row_number().over(latest_window))
            .filter((F.col("row_number") == 1) & (F.col("operation") != "D"))
            .drop("row_number")
            .collect()
        )
        if row["order_id"] in current_ids
    ]

    if len(source_rows) < 5:
        raise ValueError("At least five current orders are required for CDC mode.")

    # Keep every CDC batch educational: mostly updates, plus inserts and deletes.
    insert_count = max(1, round(CDC_BATCH_SIZE * 0.20))
    delete_count = max(1, round(CDC_BATCH_SIZE * 0.10))
    update_count = CDC_BATCH_SIZE - insert_count - delete_count
    if update_count + delete_count > len(source_rows):
        raise ValueError("cdc_batch_size is larger than the available current orders.")

    selected = random.sample(source_rows, update_count + delete_count)
    update_sources = selected[:update_count]
    delete_sources = selected[update_count:]
    # This small state machine advances ordinary orders through fulfillment.
    transition = {
        "PLACED": "PACKED",
        "PACKED": "SHIPPED",
        "SHIPPED": "DELIVERED",
        "DELAYED": "DELIVERED",
        "DELIVERED": "DELIVERED",
        "CANCELLED": "CANCELLED",
    }
    event_time = datetime.utcnow().replace(microsecond=0)
    rows = []

    # Build update events from the last known full record; force one new delay so
    # the dashboard always has a visible exception after a CDC run.
    for index, source in enumerate(update_sources, start=1):
        new_status = "DELAYED" if index == 1 else transition[source["order_status"]]
        promised_at = source["promised_delivery_at"]
        actual_at = source["actual_delivery_at"]
        if new_status == "DELAYED":
            promised_at = event_time - timedelta(hours=6)
        elif new_status == "DELIVERED":
            actual_at = event_time

        rows.append(
            {
                **{
                    key: source[key]
                    for key in source
                    if key not in {"event_id", "batch_id", "operation"}
                },
                "event_id": f"EVT-B{CDC_BATCH_ID:03d}-U{index:03d}",
                "batch_id": CDC_BATCH_ID,
                "operation": "U",
                "order_status": new_status,
                "promised_delivery_at": promised_at,
                "actual_delivery_at": actual_at,
                "event_timestamp": event_time + timedelta(seconds=index),
            }
        )

    # Delete events retain the previous payload for transparent source inspection.
    for index, source in enumerate(delete_sources, start=1):
        rows.append(
            {
                **{
                    key: source[key]
                    for key in source
                    if key not in {"event_id", "batch_id", "operation"}
                },
                "event_id": f"EVT-B{CDC_BATCH_ID:03d}-D{index:03d}",
                "batch_id": CDC_BATCH_ID,
                "operation": "D",
                "event_timestamp": event_time + timedelta(
                    seconds=update_count + index
                ),
            }
        )

    # Allocate new identifiers above the highest order number already generated.
    all_order_ids = [
        row["order_id"]
        for row in spark.table(BRONZE_EVENTS).select("order_id").distinct().collect()
    ]
    max_order_number = max(int(order_id.split("-")[1]) for order_id in all_order_ids)
    first_names = ["Anaya", "David", "Meera", "Rohan"]
    last_names = ["Iyer", "Martin", "Singh", "Wilson"]

    # New orders enter the lifecycle in PLACED state.
    for index in range(1, insert_count + 1):
        order_number = max_order_number + index
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        created_at = event_time + timedelta(
            seconds=update_count + delete_count + index
        )
        rows.append(
            {
                "event_id": f"EVT-B{CDC_BATCH_ID:03d}-I{index:03d}",
                "batch_id": CDC_BATCH_ID,
                "order_id": f"ORD-{order_number:05d}",
                "operation": "I",
                "customer_name": f"{first_name} {last_name}",
                "customer_email": f"{first_name.lower()}.{last_name.lower()}{order_number}@example.com",
                "region": random.choice(REGIONS),
                "product_category": random.choice(CATEGORIES),
                "order_amount": money(random.uniform(20, 600)),
                "order_status": "PLACED",
                "order_created_at": created_at,
                "promised_delivery_at": created_at + timedelta(days=3),
                "actual_delivery_at": None,
                "event_timestamp": created_at,
            }
        )

    return rows, CDC_BATCH_ID


# COMMAND ----------

# -----------------------------------------------------------------------------
# Generate and persist the requested batch
# -----------------------------------------------------------------------------
event_rows, generated_batch_id = (
    initial_events() if LOAD_MODE == "initial" else incremental_events()
)
events_df = spark.createDataFrame(event_rows, schema=BRONZE_SCHEMA_DEFINITION)

if LOAD_MODE == "initial":
    # MERGE makes deterministic batch 1 safe to rerun without duplicate events.
    events_df.createOrReplaceTempView("generated_order_events")
    spark.sql(
        f"""
        MERGE INTO {BRONZE_EVENTS} AS target
        USING generated_order_events AS source
          ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
else:
    # Incremental IDs were checked above, so append preserves the source event log.
    events_df.write.mode("append").saveAsTable(BRONZE_EVENTS)

# Task values are optional observability for Lakeflow; interactive runs continue
# normally because taskValues is unavailable outside a Job.
try:
    dbutils.jobs.taskValues.set(key="batch_id", value=generated_batch_id)
    dbutils.jobs.taskValues.set(key="event_count", value=len(event_rows))
except Exception:
    pass

# Show the batch mix as a quick manual verification.
display(events_df.groupBy("batch_id", "operation").count().orderBy("operation"))
