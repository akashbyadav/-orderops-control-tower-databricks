# Notebook guide

The notebooks are grouped by responsibility. Only the two `setup` notebooks are run manually during the first deployment. The Lakeflow Job executes the five operational notebooks in dependency order.

| Folder | Notebook | Reads | Writes | Execution |
|---|---|---|---|---|
| `shared` | `00_config.py` | Job parameters or interactive widgets | Python variables only | Included with `%run` by every other notebook |
| `setup` | `01_create_catalog_and_schemas.py` | `catalog_name` | Catalog and four schemas | Manual, once |
| `setup` | `02_create_tables.py` | Four schemas | Five managed Delta tables | Manual, once |
| `data_generation` | `01_generate_order_events.py` | Job parameters, and current tables in CDC mode | `bronze.order_events` | Job task 1 |
| `cdc` | `01_apply_events_to_current.py` | Bronze events | `silver.orders_current` | Job task 2 |
| `cdc` | `02_publish_order_history.py` | Silver CDF and gold history checkpoint | `gold.order_history` | Job task 3 |
| `cdc` | `03_refresh_order_metrics.py` | Silver current state | `gold.order_metrics` | Job task 4 |
| `quality` | `01_validate_pipeline.py` | Bronze, silver, and gold | No tables; raises on failure | Job task 5 |

## Interactive execution

For investigation, attach a notebook to serverless compute and run it directly. The shared configuration creates widgets with the same names used by the Job. Preserve the operational order shown above; downstream notebooks assume their upstream tables are current.

## Replacing the synthetic source

Only `data_generation/01_generate_order_events.py` is demonstration-specific. A real ingestion notebook can replace it if it writes the same bronze columns and preserves unique `event_id`, increasing event timestamps, and the `I`/`U`/`D` operation contract. The remaining CDC tasks do not change.

