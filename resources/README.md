# Bundle resource guide

The root `databricks.yml` includes both files in this directory.

| File | Deployed resources |
|---|---|
| `orderops_cdc.job.yml` | One parameterized, five-task serverless Lakeflow Job |
| `orderops_apps.yml` | One MLflow experiment, the MCP App, the Streamlit/LangGraph App, and their least-privilege resource assignments |

## Deployment-time variables

- `catalog_name` defaults to `orderops` and must match the catalog created by the setup notebooks.
- `sql_warehouse_id` resolves by looking up a warehouse named `OrderOps Warehouse`. Override the variable with an ID if the warehouse cannot be renamed.
- `llm_endpoint_name` defaults to `databricks-meta-llama-3-1-8b-instruct`. Override it with an endpoint that appears as tools-enabled in the workspace's AI Playground.

## Run-time parameters

The Lakeflow Job owns parameters that may change for each run: `load_mode`, `initial_order_count`, `cdc_batch_id`, and `cdc_batch_size`. They should be passed after `--` to `databricks bundle run`; they are not Bundle variables.

The App resource keys in `orderops_apps.yml` must match the `valueFrom` values in each App's `app.yaml`. Changing one side without the other causes a missing environment variable at App startup.
