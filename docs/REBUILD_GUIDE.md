# Rebuild guide — AtliQ Commerce from scratch

A step-by-step checklist to rebuild the whole pipeline on a fresh Azure subscription, with the settings that
matter and the problems hit the first time (and their fixes). Region used throughout: **UAE North**.
Allow about 2–3 days of work. Estimated cost if you pause/delete as you go: **$15–30**.

> **Cost rules (learned the hard way)**
> - A Fabric capacity bills **every hour while Active** (F2 ≈ $10/day). Pause it at the end of **every** session.
> - Creating a Databricks workspace also creates a **NAT gateway** in its managed resource group: ≈ $1/day,
>   24/7, until the workspace is deleted. Delete the workspace when you are done.
> - Set a budget with email alerts before you start (Cost Management → Budgets).

## 0. Before you start

| Need | Note |
|---|---|
| Azure subscription | Pay-as-you-go (the free credit expires; upgrade with the billing account) |
| GitHub account | repo with this code |
| Python 3.12 | for dbt (`py -3.12 -m venv .venv-dbt`); the simulator works on newer Python too |
| VS Code + mssql extension | to run the SQL scripts |
| ODBC Driver 18 for SQL Server | for the simulator |
| Source files | `data/` (CSV + 184 clickstream JSON files), not in git |

All resources go in one resource group, `rg-atliq-commerce`, so teardown is one delete.

## 1. Source database — Azure SQL (phase M1)

1. Create **Azure SQL Database** → apply the **free offer** → serverless General Purpose, auto-pause on.
   Server `atliq-sql-meet`, admin `atliq_admin`, database `atliq_commerce`.
2. Server → **Networking**: **Add your client IPv4 address** and tick **Allow Azure services and resources
   to access this server** → Save. (Your home IP changes: re-add it when you get a firewall error.)
3. VS Code → connect (connect timeout **120** s: the serverless DB is asleep at first) → run `sql/01` … `07`
   **one at a time** (never two in parallel) → `sql/99_checks.sql` for row counts.
4. Simulator: `copy simulator\.env.example simulator\.env` → fill in the password (never commit `.env`) →
   `python simulator\daily_order_simulator.py --orders 20`.

| Problem | Fix |
|---|---|
| Database got a default name (`free-sql-db-…`) | `ALTER DATABASE [old] MODIFY NAME = [atliq_commerce];` from `master` |
| "Client with IP … is not allowed" | Networking → add client IP → Save |
| Timeout / "not currently available" | DB waking up: wait 30–60 s, retry |
| A big insert script inserted 0 rows | Scripts are split into 1,000-row `GO` batches; run them whole, one at a time |
| `python` not found on Windows | use `py` (`py -m venv .venv`) |

## 2. Ingestion — ADLS + Data Factory (phase M2)

1. **Storage account** `atliqlakemeet`: Advanced tab → **Enable hierarchical namespace** (ADLS Gen2).
   This cannot be switched on later; if you forget, delete and recreate.
2. Container `lakehouse` → upload `landing/csv/*.csv` and `landing/clickstream/*.json`.
3. **Data Factory** `atliq-adf-meet` in the **same region** → Studio → import from [`adf/`](../adf/README.md)
   or rebuild: linked services `ls_azure_sql`, `ls_adls`; datasets `ds_sql_table`, `ds_bronze_parquet`,
   `ds_lake_binary`; pipeline `pl_nightly_ingest` (control-table driven, watermark at run start, retries 3 × 60 s).
4. **Validate all → Publish all** → Debug.

| Problem | Fix |
|---|---|
| Linked service test fails | SQL DB asleep — wait and retry |
| "Not an ancestor" on an activity | connect the green success arrow from the previous activity |
| A rerun copied 0 rows into today's batch | watermarks already moved: `UPDATE etl.control_table SET last_loaded_at='1900-01-01' WHERE load_type='incremental';` |

## 3. Silver — Databricks + Unity Catalog (phase M3)

1. **Azure Databricks** `atliq-dbx-meet`, **Premium** tier (Unity Catalog and serverless need Premium).
2. **Access connector** `atliq-dbx-connector` → on the storage account, IAM → role
   **Storage Blob Data Contributor** → the connector's managed identity.
3. Databricks → Catalog → **Storage credential** `atliq_lake_cred` (the connector's resource ID) →
   **External location** `atliq_lakehouse` = `abfss://lakehouse@atliqlakemeet.dfs.core.windows.net/`
   (if the file-events test fails, use **Force create** — that feature is optional).
4. Catalog `atliq`, schemas `silver`, `gold`.
5. Import the notebooks in [`databricks/`](../databricks/README.md) into one folder → run `02` → `03`
   (widget `run_date` = `ALL` the first time) → `04` → `05` (17/17 checkpoints).

| Problem | Fix |
|---|---|
| "Exhausted available credits" / trial limits | upgrade the workspace to Premium (portal or `az databricks workspace update --sku premium`) |
| Subscription disabled ("free credit expired") | upgrade to pay-as-you-go with the billing account |
| Import accepts only one file at a time | import the files one by one |

## 4. Gold — dbt (phase M4)

```
py -3.12 -m venv .venv-dbt
.venv-dbt\Scripts\activate
pip install -r atliq_gold\requirements.txt
setx DATABRICKS_HOST "adb-….azuredatabricks.net"
setx DATABRICKS_HTTP_PATH "/sql/1.0/warehouses/…"      # SQL warehouse → Connection details
setx DATABRICKS_TOKEN "dapi…"                          # Settings → Developer → Access tokens
# close and reopen the terminal, then:
cd atliq_gold && dbt debug && dbt build                # 13 models + 36 tests
```

| Problem | Fix |
|---|---|
| The wrong venv activates in VS Code | `deactivate`, then `.venv-dbt\Scripts\activate` |
| dbt cannot see the env vars | `setx` applies to **new** terminals only: restart VS Code |

## 5. Orchestration (phase M5)

1. Databricks → Workspace → **Git folder** linked to the GitHub repo (fine-grained, read-only token).
2. Job `atliq_nightly` — settings in [`databricks/job_atliq_nightly.md`](../databricks/job_atliq_nightly.md).
3. ADF: linked service `ls_databricks` (authentication: managed identity; grant the Data Factory's managed
   identity **Contributor** on the Databricks workspace), parameter `run_start_at` on `pl_nightly_ingest`,
   master pipeline `pl_nightly` (Set variable → Execute Pipeline → Databricks Job with `run_date`).
4. Schedule trigger `tr_nightly_0100` (daily 01:00). **Publish all** — a trigger only runs after publishing.
5. Alert: Monitor → Alerts & metrics → Failed pipeline runs ≥ 1 → action group with your email.
6. Proof: run twice with no new data → `fact_sales` count and revenue unchanged.

| Problem | Fix |
|---|---|
| Alert action group: "subscription is not registered to use namespace microsoft.insights" | Subscription → Resource providers → select **microsoft.insights** (the circle) → Register |
| Trigger has no Stop icon | click the trigger → Activated: No → OK → Publish all |

## 6. Dashboard — Fabric (phase M6)

1. Fabric capacity (F2) **or** a Fabric trial (the trial was not offered on this tenant — only a Power BI trial).
2. Workspace on that capacity → Lakehouse `lh_atliq_gold` (no schemas) → **New table shortcut** →
   ADLS Gen2 → `https://atliqlakemeet.dfs.core.windows.net` → tick the 5 folders under `lakehouse/gold`.
3. SQL analytics endpoint → **New semantic model** (5 tables) → relationships and measures as in
   [`fabric/README.md`](../fabric/README.md). In the web editor the formula bar takes the formula only;
   the name goes in Properties.
4. New report → import theme `fabric/atliq_theme.json` (Design → Import theme) → build the 2 pages.
5. File → Export → PDF. **Pause the capacity.**

| Problem | Fix |
|---|---|
| "(Blank)" in slicers | checked: no NULLs, no orphan keys → cosmetic; filter out "(Blank)" per slicer |
| Net line above gross line | Net Revenue was on the secondary y-axis — move it into the Y-axis well |
| Bar chart scrolls | give it more height; turn off X-axis values (data labels carry the numbers) |
| "Capacity isn't available" | the capacity is paused — resume it in the Azure portal |

## 7. CI (phase M7)

GitHub repo → Settings → Secrets and variables → Actions → add `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
`DATABRICKS_TOKEN` (a separate 90-day token). The workflow [`.github/workflows/dbt_ci.yml`](../.github/workflows/dbt_ci.yml)
runs `dbt build --target ci` on every dbt change (schema `atliq.ci`, never production Gold).

## 8. Teardown

Delete the resource groups `rg-atliq-commerce` and the Fabric capacity's group. In GitHub, disable the
dbt CI workflow (Actions → dbt CI → ⋯ → Disable) once Databricks is gone.

## Keeping the data after teardown

Gold was exported with AzCopy in Cloud Shell (`azcopy copy "https://<account>.dfs.core.windows.net/lakehouse/gold?<SAS>" ~/export --recursive`).
Read it as **Delta** (e.g. Python `deltalake`, DuckDB `delta_scan`), not as "all Parquet files": each table
folder still holds older versions from earlier dbt runs, so reading every Parquet file counts rows many times over.
