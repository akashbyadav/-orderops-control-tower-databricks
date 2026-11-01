# Databricks notebook source
# MAGIC %run ../shared/00_config

# COMMAND ----------

"""Create the project namespace before the bundle assigns App table resources."""

# This bootstrap runs before bundle deployment because App resource grants refer
# to these Unity Catalog objects by name.
catalog_identifier = quote_identifier(CATALOG)

# IF NOT EXISTS makes the notebook safe to rerun in the same workspace.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_identifier}")

# Separate schemas make the medallion layers and agent audit boundary visible.
for schema_name in ["bronze", "silver", "gold", "monitoring"]:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog_identifier}.`{schema_name}` "
        f"COMMENT 'OrderOps Control Tower {schema_name} schema'"
    )

# Finish with a visible verification result for both manual and Job-driven runs.
display(spark.sql(f"SHOW SCHEMAS IN {catalog_identifier}"))
