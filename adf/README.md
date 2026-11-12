# ADF — `pl_nightly_ingest` (M2) + `pl_nightly` orchestration (M5)

Exported from ADF Studio (Manage → ARM template → Export). Secrets are **not** in these files:
the SQL password and the storage account key are empty secure parameters.

| File | What it is |
|---|---|
| `ARMTemplateForFactory.json` | Linked services, datasets, pipelines and the trigger, as code |
| `ARMTemplateParametersForFactory.json` | Environment values (server, database, storage URL); secrets left blank |

## Objects

| Type | Name | Purpose |
|---|---|---|
| Linked service | `ls_azure_sql` | Azure SQL `atliq_commerce` (SQL auth) |
| Linked service | `ls_adls` | ADLS Gen2 `atliqlakemeet` (account key; Managed Identity would be the production choice) |
| Dataset | `ds_sql_table` | Any SQL table — parameters `schema`, `table_name` |
| Dataset | `ds_bronze_parquet` | Parquet in the lake — parameters `folder_path`, `file_name` |
| Dataset | `ds_lake_binary` | Any lake folder, copied byte-for-byte — parameter `folder_path` |
| Linked service | `ls_databricks` | Databricks workspace `atliq-dbx-meet` (ADF managed identity) |
| Pipeline | `pl_nightly_ingest` | The nightly ingestion (below) |
| Pipeline | `pl_nightly` | Master pipeline: ingest, then the Databricks job (M5) |
| Trigger | `tr_nightly_0100` | Schedule, daily at 01:00 (UTC+4) → `pl_nightly` |

## How the pipeline works

1. **Set run_start_at** = the `run_start_at` parameter when the master pipeline passes one, otherwise
   `@formatDateTime(utcnow(), 'yyyy-MM-dd HH:mm:ss')` — the run's **start** time (UTC).
   Watermarks advance to this value, never to "now at the end", so rows written during the run are not skipped.
2. **Lookup control table** reads all rows of `etl.control_table` (*First row only* off).
3. **ForEach table** (sequential — the serverless DB is small) → **If incremental** `@equals(item().load_type, 'incremental')`:
   - **True** — `Copy incremental`, source query
     `SELECT * FROM [schema].[table] WHERE [watermark_column] > 'last_loaded_at'`,
     sink `bronze/<table>/ingest_date=<run date>/<table>.parquet`;
     then **Update watermark** (`etl.usp_update_watermark`) on success only.
   - **False** — `Copy full` → `bronze/<table>/<table>.parquet` (overwrite).
4. **Get CSV files** (child items of `landing/csv`) → **Filter csv** (`.csv` only) → **ForEach csv** →
   binary copy to `bronze/<file name without .csv>/`.
5. **Copy clickstream** — binary copy of `landing/clickstream/*.json` to `bronze/clickstream/`.
   Not parsed in ADF on purpose: some lines are malformed; Silver quarantines them.

## Reliability choices

- **Retry 3 × 60 s** on every activity that touches SQL — the serverless database auto-pauses and
  the first connection after idle fails while it wakes.
- **Watermark moves only after a successful copy** (green "on success" arrow) — a failed night is
  retried from the same point.
- **Fixed file names** — a same-day rerun overwrites that day's batch instead of adding a duplicate file.
- **Backfill:** `UPDATE etl.control_table SET last_loaded_at = '1900-01-01' WHERE load_type = 'incremental';`
  then run the pipeline to reload everything.

## `pl_nightly` — the master pipeline (M5)

```
Set run_start_at (UTC) ─► Execute Pipeline pl_nightly_ingest (wait, passes run_start_at)
                       ─► Databricks Job atliq_nightly (job parameter run_date = first 10 chars of run_start_at)
```

- One timestamp for the whole night: the watermark, the Bronze `ingest_date` folder and the Silver `run_date` all agree.
- The Databricks job runs `02_silver_full` → `03_silver_incremental` → `04_silver_clickstream` → `dbt build`
  (notebooks and dbt from the GitHub repo, serverless compute).
- **Alert:** `alert_pipeline_failed` (Failed pipeline runs ≥ 1, 5-minute window, `pl_nightly` and
  `pl_nightly_ingest`) → action group `ag_atliq_email` → email. Needs the `Microsoft.Insights` resource provider
  registered on the subscription.
