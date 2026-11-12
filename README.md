# AtliQ Commerce — End-to-End Data Engineering Capstone

Nightly OLTP → OLAP sync: Azure SQL → ADF → ADLS (Bronze/Silver/Gold) → Databricks + dbt → Microsoft Fabric.

![Architecture](docs/architecture.svg)

## Repository layout

| Folder | Milestone | Contents |
|---|---|---|
| `sql/` | M1 | OLTP schema, seed data, ETL control table |
| `simulator/` | M1 | Daily order simulator (`.env.example` → `.env`) |
| `adf/` | M2 | Exported ADF pipeline JSON (generic, metadata-driven) |
| `databricks/` | M3 | PySpark notebooks: Bronze → Silver (cleaning, MERGE, dq_log) |
| `atliq_gold/` | M4 | dbt project: staging + Gold star schema + tests |
| `fabric/` | M6 | Dashboard screenshots / exports |
| `docs/` | all | Architecture, data quality report, idempotency proof |
| `data/` | — | Raw source files to upload to ADLS (git-ignored) |

## Progress

- [x] M1 — OLTP database in Azure SQL
- [x] M2 — Ingest into Bronze with ADF
- [x] M3 — Clean and load Silver (PySpark + Delta)
- [x] M4 — Gold star schema with dbt
- [x] M5 — Nightly sync, idempotency proof
- [ ] M6 — Fabric dashboard
- [ ] M7 — CI/CD and reliability

## M1 — OLTP database (Azure SQL)

- Azure SQL Database `atliq_commerce` on server `atliq-sql-meet` (General Purpose serverless, free offer, UAE North),
  resource group `rg-atliq-commerce`.
- Load order: `sql/01` → `07`, one script at a time. Scripts 04–06 are split into 1,000-row batches
  (`GO` between batches) so a large load can't time out as a single batch; `05` truncates `order_items` first, so it is re-runnable.
- Raw row counts after load: customers 1,008 · products 60 · orders 10,000 · order_items 23,155 · payments 8,320
  (raw — duplicates and QA test accounts are cleaned later, in Silver/staging).
- Profiling note: Azure SQL's default collation is case-insensitive and ignores trailing spaces, so `GROUP BY status`
  shows 10 values where the data holds 25 spellings (cities: 23 shown vs 54 actual). Profile text columns in Spark.

### Daily order simulator

```
py -m venv .venv
.venv\Scripts\activate
pip install pyodbc python-dotenv
copy simulator\.env.example simulator\.env   # then fill in the real password
python simulator\daily_order_simulator.py --orders 20
```
Requires ODBC Driver 18 for SQL Server. The serverless database auto-pauses; the first connection after idle can time out — wait ~30 s and retry.

## M2 — Ingest into Bronze (Azure Data Factory)

One generic, metadata-driven pipeline, `pl_nightly_ingest`, copies every source into the lake
(`atliqlakemeet` / container `lakehouse`, ADLS Gen2, UAE North). Details: [`adf/README.md`](adf/README.md).

```
                    ┌─► Lookup etl.control_table ─► ForEach table ─► If incremental?
                    │                                    True:  Copy (WHERE watermark > last_loaded_at) ─► usp_update_watermark
Set run_start_at ───┤                                    False: Copy full table (overwrite)
 (UTC, run START)   ├─► Get Metadata landing/csv ─► Filter *.csv ─► ForEach ─► Copy (binary)
                    └─► Copy landing/clickstream/*.json (binary, unchanged)
```

Bronze layout after a run:

| Source | Bronze path | Load |
|---|---|---|
| customers, products | `bronze/<table>/<table>.parquet` | full, overwritten nightly |
| orders, order_items, payments | `bronze/<table>/ingest_date=YYYY-MM-DD/<table>.parquet` | incremental (watermark) |
| supplier_price_list, marketing_spend | `bronze/<name>/<name>.csv` | full, copied as-is |
| clickstream (184 files) | `bronze/clickstream/*.json` | copied as-is (binary) — parsed in Silver |

Verified on the first run: 22/22 activities succeeded; orders 10,020 rows read = written;
watermarks moved to the run start (same value for all three tables); a second run with no
source changes copied 0 rows (incremental works); a same-day rerun replaced the batch file
instead of duplicating it (fixed file names).

## M3 — Clean and load Silver (Databricks, PySpark + Delta, Unity Catalog)

Workspace `atliq-dbx-meet` (Premium, UAE North), serverless compute, catalog `atliq` with schemas `silver` / `gold`.
The lake is reached through Unity Catalog (access connector → storage credential → external location), not mounts or keys.
Notebooks and run order: [`databricks/README.md`](databricks/README.md).

- One shared rulebook (`cleaning_rules`): normalise text, then map it; anything unmapped becomes NULL and is counted.
- Full tables are cleaned and overwritten; orders, order items and payments are MERGEd on their business keys
  (update only when newer), reading one night's Bronze batch via `run_date` (or `ALL` to rebuild).
- Clickstream: 426 malformed lines quarantined, re-sent events and whole bot sessions removed.
- Every rule logs its row count to `atliq.silver.dq_log`; **17 of 17 checkpoint totals match** the guide.
  Full report: [`docs/data_quality_report.md`](docs/data_quality_report.md).

## M4 — Gold star schema (dbt Core on Databricks)

dbt project: [`atliq_gold/`](atliq_gold/README.md) (dbt-core 1.12 + dbt-databricks 1.13, Python 3.12 in `.venv-dbt`).
Connection from environment variables only; `profiles.yml` has `dev` (schema `gold`) and `ci` (schema `ci`) targets.

- **Staging (views):** QA test accounts removed, double-submitted lines de-duplicated, first payment per order,
  current supplier cost (latest effective_date).
- **Gold (external Delta tables at `lakehouse/gold/`, readable by Fabric):** `fact_sales` (one row per order item),
  `dim_customer` (+ first_order_date), `dim_product` (+ supplier cost, unit margin), `dim_date` (rolls forward
  automatically), `fact_web_sessions` (one row per session, funnel flags).
- `dbt build`: **13 models + 36 tests, PASS=49** (unique, not_null, accepted_values, relationships, and the
  stretch-goal test `assert_payments_match_orders`).
- Gold totals for the seed period match the checkpoints: 22,959 sales rows, gross revenue 32,424,661.00,
  returned 4,574,944.00, 1,000 customers, 60 products (supplier cost 56,871.53), funnel 23,193 → 13,309 → 7,391 → 4,442 → 3,446.

## M5 — Nightly sync and idempotency

One schedule runs everything: ADF trigger `tr_nightly_0100` (01:00 daily, UTC+4) → `pl_nightly`
→ `pl_nightly_ingest` → Databricks job `atliq_nightly` (Silver notebooks → `dbt build`). Details: [`adf/README.md`](adf/README.md).

```
tr_nightly_0100 ─► pl_nightly ─► pl_nightly_ingest (SQL → Bronze)
                               └► Databricks job atliq_nightly
                                    02_silver_full → 03_silver_incremental → 04_silver_clickstream → dbt build (Gold)
```

- **Code from Git:** the Databricks job runs notebooks and the dbt project from a Git folder linked to this repo.
- **One run date:** `pl_nightly` sets `run_start_at` once and passes it to ingest (watermark + Bronze folder) and to the
  job (`run_date`), so every layer processes the same night's batch.
- **Change test:** the simulator added 20 orders and moved 5 existing orders to `Returned`
  (`--update-existing`). After one `pl_nightly` run, Silver had 10,040 orders (10,020 + 20) and the 5 orders were
  **updated in place** (still 5 rows, new `updated_at`), not duplicated.
- **Idempotency proof:** the full pipeline was run a second time with no new source data. Gold `fact_sales` was
  identical: **23,051 rows, gross revenue 39,728,680.00** both times.
  Screenshots: [`docs/idempotency_before.png`](docs/idempotency_before.png), [`docs/idempotency_after.png`](docs/idempotency_after.png).
- **Failure alert:** Azure Monitor alert on failed pipeline runs → email.
- Note: the M3/M4 checkpoint totals were captured on the seed data; simulator runs (new orders, status changes)
  change the Gold totals afterwards, as expected.
