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

- [ ] M1 — OLTP database in Azure SQL
- [ ] M2 — Ingest into Bronze with ADF
- [ ] M3 — Clean and load Silver (PySpark + Delta)
- [ ] M4 — Gold star schema with dbt
- [ ] M5 — Nightly sync, idempotency proof
- [ ] M6 — Fabric dashboard
- [ ] M7 — CI/CD and reliability
