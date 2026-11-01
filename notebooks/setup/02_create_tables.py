# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Create the five Delta tables used by the workflow and Databricks Apps."""

# Bronze is an immutable event log. It intentionally retains synthetic PII so
# the silver transformation can demonstrate masking before serving consumers.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_EVENTS} (
      event_id STRING NOT NULL,
      batch_id INT NOT NULL,
      order_id STRING NOT NULL,
      operation STRING NOT NULL COMMENT 'I=insert, U=update, D=delete',
      customer_name STRING,
      customer_email STRING,
      region STRING,
      product_category STRING,
      order_amount DECIMAL(12, 2),
      order_status STRING,
      order_created_at TIMESTAMP,
      promised_delivery_at TIMESTAMP,
      actual_delivery_at TIMESTAMP,
      event_timestamp TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Append-only synthetic order CDC events'
    """
)

# COMMAND ----------

# Silver stores only the latest state per order. Change Data Feed is enabled here
# because the next notebook derives business history from each MERGE commit.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_CURRENT} (
      order_id STRING NOT NULL,
      customer_name_masked STRING,
      customer_email_masked STRING,
      region STRING,
      product_category STRING,
      order_amount DECIMAL(12, 2),
      order_status STRING,
      order_created_at TIMESTAMP,
      promised_delivery_at TIMESTAMP,
      actual_delivery_at TIMESTAMP,
      updated_at TIMESTAMP,
      source_event_id STRING,
      source_batch_id INT
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    COMMENT 'Current masked order state with Delta Change Data Feed enabled'
    """
)

# COMMAND ----------

# Gold history converts technical CDF pre/post images into one business change.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {GOLD_HISTORY} (
      order_id STRING NOT NULL,
      change_type STRING NOT NULL,
      commit_version BIGINT NOT NULL,
      commit_timestamp TIMESTAMP NOT NULL,
      old_status STRING,
      new_status STRING,
      region STRING,
      order_amount DECIMAL(12, 2)
    )
    USING DELTA
    COMMENT 'Business history derived from the silver Delta Change Data Feed'
    """
)

# Gold metrics is a compact current snapshot used by dashboards and MCP tools.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {GOLD_METRICS} (
      metric_date DATE NOT NULL,
      region STRING NOT NULL,
      order_status STRING NOT NULL,
      order_count BIGINT,
      total_amount DECIMAL(18, 2),
      delayed_count BIGINT,
      refreshed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Current operational metrics by date, region, and status'
    """
)

# COMMAND ----------

# The user-facing App can write only this metadata table. Prompts, answers, names,
# and email addresses are deliberately absent from its schema.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {AGENT_AUDIT} (
      request_id STRING NOT NULL,
      event_time TIMESTAMP NOT NULL,
      question_category STRING,
      tool_name STRING,
      outcome STRING,
      latency_ms BIGINT
    )
    USING DELTA
    COMMENT 'Minimal metadata written once per agent request'
    """
)
