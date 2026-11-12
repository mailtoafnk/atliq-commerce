-- drop double-submitted lines: keep one row per order, product, quantity, price and created_at
SELECT order_item_id, order_id, product_id, quantity, item_price, created_at
FROM {{ source('silver', 'order_items') }}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id, product_id, quantity, item_price, created_at
  ORDER BY order_item_id) = 1
