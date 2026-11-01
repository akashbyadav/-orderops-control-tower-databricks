# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Convert unprocessed silver Change Data Feed versions into order history."""

# -----------------------------------------------------------------------------
# Step 1: calculate the unprocessed Delta version range
# -----------------------------------------------------------------------------
# Gold's maximum commit version doubles as a lightweight processing checkpoint.
latest_source_version = (
    spark.sql(f"DESCRIBE HISTORY {SILVER_CURRENT}")
    .selectExpr("MAX(version) AS version")
    .first()["version"]
)
last_processed_version = spark.sql(
    f"SELECT COALESCE(MAX(commit_version), -1) AS version FROM {GOLD_HISTORY}"
).first()["version"]
start_version = last_processed_version + 1

print(
    f"last_processed_version={last_processed_version}, "
    f"latest_source_version={latest_source_version}"
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# Step 2: read and translate only new Change Data Feed commits
# -----------------------------------------------------------------------------
processed_change_count = 0

if start_version <= latest_source_version:
    # table_changes returns insert/delete rows and update pre/post image pairs.
    changes = spark.sql(
        f"""
        SELECT *
        FROM table_changes(
          '{SILVER_CURRENT}',
          {start_version},
          {latest_source_version}
        )
        """
    )
    changes.createOrReplaceTempView("new_order_cdf")

    # Collapse technical preimage/postimage pairs into one understandable business
    # row with old/new status and a stable (order, commit version) identity.
    business_changes = spark.sql(
        """
        SELECT
          order_id,
          CASE
            WHEN MAX(CASE WHEN _change_type = 'delete' THEN 1 ELSE 0 END) = 1
              THEN 'delete'
            WHEN MAX(CASE WHEN _change_type = 'update_postimage' THEN 1 ELSE 0 END) = 1
              THEN 'update'
            ELSE 'insert'
          END AS change_type,
          _commit_version AS commit_version,
          _commit_timestamp AS commit_timestamp,
          MAX(CASE
                WHEN _change_type IN ('update_preimage', 'delete') THEN order_status
              END) AS old_status,
          MAX(CASE
                WHEN _change_type IN ('update_postimage', 'insert') THEN order_status
              END) AS new_status,
          COALESCE(
            MAX(CASE WHEN _change_type IN ('update_postimage', 'insert') THEN region END),
            MAX(CASE WHEN _change_type IN ('update_preimage', 'delete') THEN region END)
          ) AS region,
          COALESCE(
            MAX(CASE WHEN _change_type IN ('update_postimage', 'insert') THEN order_amount END),
            MAX(CASE WHEN _change_type IN ('update_preimage', 'delete') THEN order_amount END)
          ) AS order_amount
        FROM new_order_cdf
        GROUP BY order_id, _commit_version, _commit_timestamp
        """
    )
    processed_change_count = business_changes.count()
    business_changes.createOrReplaceTempView("new_business_changes")

    # MERGE makes a replay safe if a run fails after writing but before completion.
    spark.sql(
        f"""
        MERGE INTO {GOLD_HISTORY} AS target
        USING new_business_changes AS source
          ON target.order_id = source.order_id
         AND target.commit_version = source.commit_version
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"Processed CDF versions {start_version} through {latest_source_version}.")
else:
    print("No new Delta versions require history processing.")

# -----------------------------------------------------------------------------
# Step 3: publish observability and preview output
# -----------------------------------------------------------------------------
try:
    dbutils.jobs.taskValues.set(key="cdf_change_count", value=processed_change_count)
    dbutils.jobs.taskValues.set(key="latest_version", value=latest_source_version)
except Exception:
    pass

# Display the full small history table so CDF effects are easy to inspect.
display(spark.table(GOLD_HISTORY).orderBy("commit_version", "order_id"))
