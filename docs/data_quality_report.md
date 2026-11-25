# Data Quality Report — Silver (phase M3)

Seed run of 2026-11-12. Source: `atliq.silver.dq_log` (latest count per rule) and `databricks/05_checkpoints`.
Every cleaning rule is implemented in `databricks/cleaning_rules` and the Silver notebooks,
and logs how many rows it touched. Rows marked **must be 0** prove that no value slipped past a lookup map.

## 1. What was wrong in Bronze (profiling, `01_profile_bronze`)

| Check | Raw value | Clean target |
|---|---|---|
| Order status spellings | 25 | 5 |
| City spellings | 54 | 10 |
| Category spellings | 17 | 6 |
| Payment method spellings | 24 | 6 |
| Marketing channel spellings | 20 | 5 |
| QA test accounts (`@atliq-test.com`) | 8 | flagged, excluded from Gold |
| Emails not lower-case/trimmed | 52 | 0 |
| Orders dated 1900-01-01 | 55 | 0 |
| Double-submitted order lines | 101 | removed in staging |
| Retried payments (extra rows) | 108 | first kept in staging |

Azure SQL's default collation hid most of this (it showed 10 statuses, 23 cities); Spark compares exactly.

## 2. What each cleaning rule did (`atliq.silver.dq_log`)

| Table | Rule | Rows |
|---|---|---:|
| customers | city: spelling/case/old name fixed | 130 |
| customers | city: unmapped (NULL) — must be 0 | 0 |
| customers | email: lower-cased and trimmed | 52 |
| customers | is_test: QA account flagged | 8 |
| products | product_name: spaces trimmed/collapsed | 5 |
| products | category: spelling/case/'and' fixed | 13 |
| products | category: unmapped (NULL) — must be 0 | 0 |
| supplier_price_list | amount: currency symbol / separators parsed | 33 |
| supplier_price_list | date: non-ISO format parsed | 40 |
| supplier_price_list | unparseable amount or date — must be 0 | 0 |
| supplier_price_list | exact duplicate rows removed | 4 |
| supplier_price_list | rows for unknown/discontinued products dropped | 3 |
| marketing_spend | exact duplicate rows removed | 54 |
| marketing_spend | rows with blank spend dropped | 17 |
| marketing_spend | channel: spelling/case/alias fixed | 473 |
| marketing_spend | channel: unmapped (NULL) — must be 0 | 0 |
| marketing_spend | clicks: thousands separator removed | 451 |
| marketing_spend | negative corrections kept (they net off) | 44 |
| orders | status: spelling/case/spaces fixed | 789 |
| orders | status: unmapped (NULL) — must be 0 | 0 |
| orders | order_date 1900-01-01 replaced by created_at date | 55 |
| orders | older versions of the same order in batch collapsed | 0 |
| order_items | double-submitted lines detected (removed in staging) | 101 |
| payments | method: spelling/case/alias fixed | 1,015 |
| payments | method: unmapped (NULL) — must be 0 | 0 |
| payments | retried payments detected (first kept in staging) | 108 |
| web_events | unparseable lines quarantined | 426 |
| web_events | event_ts given as epoch milliseconds (parsed) | 6,816 |
| web_events | event_type: case/spaces fixed | 6,833 |
| web_events | event_ts unparseable — must be 0 | 0 |
| web_events | event_type not one of the 5 — must be 0 | 0 |
| web_events | re-sent events removed (one row per event_id) | 2,680 |
| web_events | events without a session dropped | 638 |
| web_events | bot sessions dropped (sessions) | 1,059 |
| web_events | bot sessions dropped (events) | 38,414 |
| web_events | events for unknown products dropped | 186 |

Orders, order items and payments include the 20 orders added by the daily simulator before this run.

## 3. Reference checkpoint totals — 17 of 17 match

Applied with the same rules dbt staging uses in M4 (exclude test accounts, one row per duplicate line,
first payment per order), seed period up to 2026-08-31.

| Check | Expected | Actual | |
|---|---|---|---|
| Customers (excl. test) | 1,000 | 1,000 | ✅ |
| Distinct cities | 10 | 10 | ✅ |
| Products | 60 | 60 | ✅ |
| Distinct categories | 6 | 6 | ✅ |
| Orders (excl. test) | 9,961 | 9,961 | ✅ |
| Placed / Shipped / Delivered / Cancelled / Returned | 47 / 102 / 6,838 / 1,778 / 1,196 | same | ✅ |
| Order items | 22,959 | 22,959 | ✅ |
| Payments (one per paid order) | 8,183 | 8,183 | ✅ |
| Payments total | 32,424,661.00 | 32,424,661.00 | ✅ |
| Gross revenue (excl. Cancelled) | 32,424,661.00 | 32,424,661.00 | ✅ |
| Returned revenue | 4,574,944.00 | 4,574,944.00 | ✅ |
| Supplier current-cost rows | 60 | 60 | ✅ |
| Supplier current costs sum | 56,871.53 | 56,871.53 | ✅ |
| Marketing rows / spend / clicks | 3,037 / 8,920,850.16 / 3,274,996 | same | ✅ |
| Clickstream quarantined | 426 | 426 | ✅ |
| Clean events / sessions | 93,436 / 23,193 | same | ✅ |
| Funnel: product view / cart / checkout / purchase | 13,309 / 7,391 / 4,442 / 3,446 | same | ✅ |

Net revenue = 32,424,661.00 − 4,574,944.00 = **27,849,717.00** (matches the reference total).

## 4. Idempotency observed in Silver

`03_silver_incremental` was run twice with `run_date = ALL`: `atliq.silver.orders` stayed at 10,020 rows
(not 20,040). MERGE on the business key never duplicates. The dq_log is append-only by design, so it holds one
set of rows per run; the report above takes the latest row per rule.
