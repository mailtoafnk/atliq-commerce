# Fabric — M6 dashboard

| File | What it is |
|---|---|
| `atliq_dashboard.pdf` | Export of the Power BI report (both pages) |
| `dashboard_sales_overview.png` | Page 1 — Sales overview |
| `dashboard_website_funnel.png` | Page 2 — Website funnel |
| `atliq_theme.json` | Report theme (colours, card style) — View/Design → Themes → Import theme |

## How it is built

1. **Lakehouse** `lh_atliq_gold` (no schemas) in workspace `atliq-commerce`.
2. **Table shortcuts** (ADLS Gen2, connection `conn_atliq_adls`) to `lakehouse/gold/`:
   `dim_customer`, `dim_date`, `dim_product`, `fact_sales`, `fact_web_sessions`. Data stays in ADLS; nothing is copied.
3. **Semantic model** `sm_atliq_sales` (Direct Lake). Relationships, all many-to-one, single direction:
   `fact_sales.customer_id → dim_customer`, `fact_sales.product_id → dim_product`,
   `fact_sales.order_date → dim_date.date_day`, `fact_web_sessions.session_date → dim_date.date_day`.
4. **Measures**

| Measure | DAX |
|---|---|
| Gross Revenue | `CALCULATE(SUM(fact_sales[gross_revenue]), fact_sales[status] <> "Cancelled")` |
| Returned Revenue | `CALCULATE(SUM(fact_sales[gross_revenue]), fact_sales[status] = "Returned")` |
| Net Revenue | `[Gross Revenue] - [Returned Revenue]` |
| Orders | `CALCULATE(DISTINCTCOUNT(fact_sales[order_id]), fact_sales[status] <> "Cancelled")` |
| Active Customers | `CALCULATE(DISTINCTCOUNT(fact_sales[customer_id]), fact_sales[status] <> "Cancelled")` |
| New Customers | customers whose `first_order_date` falls in the selected period |
| Returning Customers | `[Active Customers] - [New Customers]` |
| Sessions / Product Views / Add to Cart / Checkouts / Purchases | `COUNTROWS` / `SUM` of the `reached_*` flags |
| Conversion Rate | `DIVIDE([Purchases], [Sessions])` |
| Cart to Purchase | `DIVIDE([Purchases], [Add to Cart])` |

## Cost

The F2 capacity is **paused** whenever the report is not being worked on (≈ $0.40/hour when running).
While paused the report cannot be opened — the PDF and PNGs here are the evidence.
A Fabric trial capacity was not available on this tenant (only a Power BI trial), so the paid F2 was used briefly.
