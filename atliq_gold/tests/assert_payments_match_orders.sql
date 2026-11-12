-- stretch goal: any row returned = test fails.
-- Every paid order's amount must equal its (de-duplicated) payment. Remove the QUALIFY in
-- stg_payments and the gateway's retried payments make this test fail.
SELECT o.order_id, o.order_amount, SUM(p.amount) AS total_paid
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
GROUP BY o.order_id, o.order_amount
HAVING SUM(p.amount) <> o.order_amount
