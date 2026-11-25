# AtliQ Commerce — Architecture (one page)

**What it does:** every night it copies new and changed orders from the shop's database into a lakehouse,
cleans them, models them into a star schema and serves them to a Power BI dashboard — safely re-runnable,
tested on every code change, and alerting by email if anything fails.

![Architecture](architecture.svg)

## Flow

```
 01:00 trigger ─► ADF pl_nightly
                   ├─ pl_nightly_ingest ── Azure SQL (watermarks) + CSV + clickstream ─► ADLS Bronze
                   └─ Databricks job atliq_nightly (code from GitHub)
                        02 Silver full ─► 03 Silver incremental (MERGE) ─► 04 clickstream ─► dbt build ─► Gold
 Gold (Delta, ADLS) ─► OneLake shortcuts ─► Direct Lake model ─► Power BI report
 git push ─► GitHub Actions: dbt build --target ci (schema atliq.ci)
```

## Components

| Layer | Service | Key choice | Why |
|---|---|---|---|
| Source (OLTP) | Azure SQL Database (serverless) | `updated_at` on every table, `etl.control_table` | Incremental loads need a change column and one place to keep watermarks |
| Ingestion | Azure Data Factory | One generic, metadata-driven pipeline | Adding a table = one row in the control table, not a new pipeline |
| Storage | ADLS Gen2 (`lakehouse` container) | Bronze / Silver / Gold folders, Delta | Open format, readable by Databricks and Fabric without copies |
| Transform | Azure Databricks + Unity Catalog | PySpark for Silver, dbt for Gold, serverless | Governance via catalog + external location, no keys in code |
| Modelling | dbt Core 1.12 | Staging views + Gold tables, 36 tests | Tested, documented SQL with lineage (`ref`, `source`) |
| Serving | Microsoft Fabric | OneLake shortcuts + Direct Lake | Reads Gold in place: no import, no refresh, no duplicate data |
| Orchestration | ADF master pipeline + schedule trigger | One `run_start_at` passed to every step | Watermark, Bronze folder and Silver run date always agree |
| CI | GitHub Actions | `dbt build --target ci` on every dbt change | Broken models or failing tests are caught before they reach Gold |
| Monitoring | Azure Monitor alert | Failed pipeline runs ≥ 1 → email | Nobody has to check the pipeline by hand |

## How it stays correct

- **Incremental:** only rows with `updated_at` > watermark are copied; watermark moves to the run's **start** time, only on success.
- **Idempotent:** fixed Bronze file names (same-day rerun overwrites), Delta `MERGE` on business keys (update only when newer),
  dbt rebuilds Gold from Silver. Proof: two full runs → `fact_sales` 23,051 rows / 39,728,680.00 both times.
- **Data quality:** text normalised then mapped; anything unmappable becomes NULL and is **counted** in `dq_log`;
  malformed clickstream lines quarantined; bot sessions removed. 17/17 checkpoint totals matched.
- **Reconciled end to end:** Databricks = Fabric SQL endpoint = Power BI (gross 32,760,622 · net 28,169,493).

## Security and cost

- No secrets in the repo: SQL password in a git-ignored `.env`, Databricks token in environment variables locally and
  **GitHub Actions secrets** in CI; lake access through a managed identity (access connector).
- CI writes only to `atliq.ci` / `lakehouse/ci/`, never to production Gold.
- Serverless SQL and compute (auto-pause), budget alert, Fabric F2 **paused** when not in use, nightly trigger
  stopped after proving it. Everything lives in one resource group, so teardown is one delete.

## What I would add for production

Key Vault for every secret and managed identity everywhere (no account key / PAT) · separate dev and prod workspaces
with CI deploying to prod · alerts on data-quality thresholds, not only failures · incremental dbt models for
`fact_sales` and partitioned `web_events` at scale · "Unknown" members in Gold instead of NULL.
