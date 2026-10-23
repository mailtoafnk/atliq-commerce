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
- [ ] M3 — Clean and load Silver (PySpark + Delta)
- [ ] M4 — Gold star schema with dbt
- [ ] M5 — Nightly sync, idempotency proof
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
