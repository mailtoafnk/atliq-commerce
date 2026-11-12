# Databricks notebook source
# MAGIC %md
# MAGIC # 05_checkpoints: compare Silver with the guide's section 7.7 totals
# MAGIC Silver keeps test accounts (flagged), double-submitted lines and retried payments. This notebook applies the
# MAGIC **same rules dbt staging will apply in M4** (exclude test accounts, keep one line per duplicate group, first
# MAGIC payment per order) so we can check the cleaning before building Gold. Seed period: up to 2026-08-31.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW chk_orders AS
# MAGIC SELECT o.* FROM atliq.silver.orders o
# MAGIC JOIN atliq.silver.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE NOT c.is_test AND o.order_date <= DATE'2026-08-31';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW chk_items AS
# MAGIC SELECT i.*, o.status FROM (
# MAGIC   SELECT * FROM atliq.silver.order_items
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id, quantity, item_price, created_at ORDER BY order_item_id) = 1
# MAGIC ) i JOIN chk_orders o ON i.order_id = o.order_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW chk_payments AS
# MAGIC SELECT p.* FROM (
# MAGIC   SELECT * FROM atliq.silver.payments
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, payment_id) = 1
# MAGIC ) p JOIN chk_orders o ON p.order_id = o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metric, actual, expected, CASE WHEN actual = expected THEN '✅' ELSE '❌' END AS ok FROM (
# MAGIC   SELECT 1 n, 'Customers (excl. test)' metric, CAST(COUNT(*) AS STRING) actual, '1000' expected FROM atliq.silver.customers WHERE NOT is_test
# MAGIC   UNION ALL SELECT 2, 'Distinct cities', CAST(COUNT(DISTINCT city) AS STRING), '10' FROM atliq.silver.customers WHERE NOT is_test
# MAGIC   UNION ALL SELECT 3, 'Products', CAST(COUNT(*) AS STRING), '60' FROM atliq.silver.products
# MAGIC   UNION ALL SELECT 4, 'Distinct categories', CAST(COUNT(DISTINCT category) AS STRING), '6' FROM atliq.silver.products
# MAGIC   UNION ALL SELECT 5, 'Orders (excl. test)', CAST(COUNT(*) AS STRING), '9961' FROM chk_orders
# MAGIC   UNION ALL SELECT 6, 'Placed / Shipped / Delivered / Cancelled / Returned',
# MAGIC     CONCAT_WS(' / ', SUM(INT(status='Placed')), SUM(INT(status='Shipped')), SUM(INT(status='Delivered')), SUM(INT(status='Cancelled')), SUM(INT(status='Returned'))),
# MAGIC     '47 / 102 / 6838 / 1778 / 1196' FROM chk_orders
# MAGIC   UNION ALL SELECT 7, 'Order items', CAST(COUNT(*) AS STRING), '22959' FROM chk_items
# MAGIC   UNION ALL SELECT 8, 'Payments (one per paid order)', CAST(COUNT(*) AS STRING), '8183' FROM chk_payments
# MAGIC   UNION ALL SELECT 9, 'Payments total', CAST(SUM(amount) AS STRING), '32424661.00' FROM chk_payments
# MAGIC   UNION ALL SELECT 10, 'Gross revenue (excl. Cancelled)', CAST(SUM(CASE WHEN status <> 'Cancelled' THEN quantity * item_price END) AS DECIMAL(14,2)) || '', '32424661.00' FROM chk_items
# MAGIC   UNION ALL SELECT 11, 'Returned revenue', CAST(SUM(CASE WHEN status = 'Returned' THEN quantity * item_price END) AS DECIMAL(14,2)) || '', '4574944.00' FROM chk_items
# MAGIC   UNION ALL SELECT 12, 'Supplier current-cost rows', CAST(COUNT(*) AS STRING), '60' FROM (
# MAGIC       SELECT * FROM atliq.silver.supplier_price_list QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) = 1)
# MAGIC   UNION ALL SELECT 13, 'Supplier current costs sum', CAST(SUM(supplier_cost) AS STRING), '56871.53' FROM (
# MAGIC       SELECT * FROM atliq.silver.supplier_price_list QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) = 1)
# MAGIC   UNION ALL SELECT 14, 'Marketing rows / spend / clicks',
# MAGIC     CONCAT_WS(' / ', COUNT(*), CAST(SUM(spend_amount) AS STRING), SUM(clicks)), '3037 / 8920850.16 / 3274996' FROM atliq.silver.marketing_spend
# MAGIC   UNION ALL SELECT 15, 'Clickstream quarantined', CAST(COUNT(*) AS STRING), '426' FROM atliq.silver.web_events_quarantine
# MAGIC   UNION ALL SELECT 16, 'Clean events / sessions',
# MAGIC     CONCAT_WS(' / ', COUNT(*), COUNT(DISTINCT session_id)), '93436 / 23193' FROM atliq.silver.web_events
# MAGIC   UNION ALL SELECT 17, 'Funnel: product view / cart / checkout / purchase',
# MAGIC     CONCAT_WS(' / ', SUM(pv), SUM(ac), SUM(co), SUM(pu)), '13309 / 7391 / 4442 / 3446' FROM (
# MAGIC       SELECT session_id, MAX(INT(event_type='product_view')) pv, MAX(INT(event_type='add_to_cart')) ac,
# MAGIC              MAX(INT(event_type='checkout')) co, MAX(INT(event_type='purchase')) pu
# MAGIC       FROM atliq.silver.web_events GROUP BY session_id)
# MAGIC ) ORDER BY n

# COMMAND ----------

# MAGIC %md ## Data quality log: the latest count for every rule
# MAGIC The log is append-only (every run adds rows), so pick the most recent row per table + rule.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, rule, rows_affected, run_date, logged_at
# MAGIC FROM atliq.silver.dq_log
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name, rule ORDER BY logged_at DESC) = 1
# MAGIC ORDER BY table_name, logged_at
