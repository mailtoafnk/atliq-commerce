# Databricks job `atliq_nightly` — settings

Started by ADF (`pl_nightly` → Databricks Job activity). No schedule of its own.

| Setting | Value |
|---|---|
| Source | Git folder linked to this repo (Workspace → Users → you → `atliq-commerce`) |
| Compute | Serverless (Performance optimized: **off**, cheaper) |
| Job parameter | `run_date` (default empty = today UTC; ADF passes `yyyy-MM-dd` of the run start) |

| # | Task | Type | Path / command | Depends on |
|---|---|---|---|---|
| 1 | `silver_full` | Notebook | `databricks/02_silver_full` | — |
| 2 | `silver_incremental` | Notebook | `databricks/03_silver_incremental` | silver_full |
| 3 | `silver_clickstream` | Notebook | `databricks/04_silver_clickstream` | silver_incremental |
| 4 | `dbt_gold` | dbt | project dir `atliq-commerce/atliq_gold`, command `dbt build`, SQL warehouse **Serverless Starter Warehouse**, catalog `atliq`, schema `gold`, profiles dir empty | silver_clickstream, run if **All succeeded** |

The notebooks read `run_date` through the `dbutils.widgets` value, which the job parameter fills.
A typical nightly run takes about 13 minutes; the ADF ingest before it about 2 minutes.
