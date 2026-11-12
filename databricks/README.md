# Databricks — Silver layer (Milestone 3)

Notebooks are stored as Databricks source files (`.py`, cells split by `# COMMAND ----------`).
Import them into one workspace folder (Workspace → folder → Import → File); `%run ./cleaning_rules` needs them side by side.
Compute: Serverless. Unity Catalog: catalog `atliq`, schemas `silver` and `gold`, storage under the external location
`atliq_lakehouse` (`abfss://lakehouse@atliqlakemeet.dfs.core.windows.net/`), credential `atliq_lake_cred`
(access connector `atliq-dbx-connector`, role Storage Blob Data Contributor on the lake). No keys in code.

| Notebook | Writes | Pattern |
|---|---|---|
| `cleaning_rules` | — | shared paths, `norm()` + 6 lookup maps, parsers, `batch_exists`, `log_dq` |
| `01_profile_bronze` | — | measures the mess (Spark shows all 25 status spellings) |
| `02_silver_full` | customers, products, supplier_price_list, marketing_spend | clean + **overwrite** |
| `03_silver_incremental` | orders, order_items, payments | this run's batch → latest per key → clean → **MERGE** |
| `04_silver_clickstream` | web_events, web_events_quarantine | parse with schema, quarantine, de-dup, drop bots → rebuild |
| `05_checkpoints` | — | the 17 checkpoint totals (✅/❌) and the dq_log report |

Run order: `02` → `03` → `04` → `05` (`02` first: supplier and clickstream check products).

## run_date (widget on 03; logged by all)

| Value | Reads |
|---|---|
| empty | today's batch, `bronze/<table>/ingest_date=<today UTC>/` |
| `YYYY-MM-DD` | that night's batch (the nightly job passes it in M5) |
| `ALL` | every `ingest_date=` folder (first build or rebuild) |

A night with no batch prints "No new … batch … Silver unchanged" instead of failing (quiet-night guard).

## Key rules

- Normalise (trim, lower, collapse spaces) then map; unmapped values become NULL and are counted ("must be 0").
- Flag, don't delete: QA accounts get `is_test`; duplicate lines and retried payments stay in Silver and are removed in dbt staging.
- Orders and payments MERGE with "update only when newer" (`s.updated_at > t.updated_at`); order items are insert-only.
- Numbers from CSV text are extracted, never character-stripped (`Rs. 1202.45` → 1202.45), and parsed with `try_` functions so ANSI mode never fails the job.
- Clickstream: drop whole bot sessions (bot, crawler, spider, python-requests), date events by `event_ts`.

Results: [`docs/data_quality_report.md`](../docs/data_quality_report.md) — 36 rules logged, 17/17 checkpoints ✅.
