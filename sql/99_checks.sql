-- M1 verification: raw row counts after loading the seed data
SELECT 'customers' AS tbl, COUNT(*) AS [rows] FROM dbo.customers
UNION ALL SELECT 'products',    COUNT(*) FROM dbo.products
UNION ALL SELECT 'orders',      COUNT(*) FROM dbo.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items
UNION ALL SELECT 'payments',    COUNT(*) FROM dbo.payments;

SELECT * FROM etl.control_table;


--TRUNCATE TABLE dbo.order_items;
--SELECT COUNT(*) AS order_items_rows FROM dbo.order_items;   -- should be 0

-- SELECT status, COUNT(*) AS n FROM dbo.orders GROUP BY status ORDER BY n DESC;
-- SELECT COUNT(DISTINCT status) AS distinct_status FROM dbo.orders;
-- SELECT COUNT(DISTINCT city)   AS distinct_city   FROM dbo.customers;

-- SELECT TOP 5 order_id, status, order_amount, created_at, updated_at
-- FROM dbo.orders ORDER BY order_id DESC;

SELECT table_name, load_type, watermark_column, last_loaded_at
FROM etl.control_table;

UPDATE etl.control_table
SET last_loaded_at = '1900-01-01'
WHERE load_type = 'incremental';

SELECT table_name, last_loaded_at FROM etl.control_table;

UPDATE etl.control_table
SET last_loaded_at = '1900-01-01'
WHERE load_type = 'incremental';