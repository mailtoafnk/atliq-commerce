# atliq_gold — dbt project (phase M4)

Builds the Gold star schema in `atliq.gold` from the cleaned Silver tables in `atliq.silver`.

```
silver.*  ──►  staging (views)                 ──►  gold (external Delta tables at lakehouse/gold/)
               stg_orders: drop QA test accounts     fact_sales        (one row per order item)
               stg_order_items: drop double lines    dim_customer      (+ first_order_date)
               stg_payments: first payment/order     dim_product       (+ current supplier cost, margin)
               stg_supplier_price_list: latest cost  dim_date          (2024-01-01 .. end of next year)
                                                     fact_web_sessions (one row per session, funnel flags)
```

## Run

```
.venv-dbt\Scripts\activate          # Python 3.12 + dbt-databricks (v1)
cd atliq_gold
dbt debug                           # checks the connection
dbt build                           # models + tests together
```

Connection comes from environment variables only (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`);
`profiles.yml` holds no secrets. Target `dev` writes to schema `gold`; target `ci` writes to `ci`, so CI never
overwrites the Gold files that Fabric reads.

Revenue definition for reports: exclude `Cancelled`; show `Returned` separately from net revenue.
