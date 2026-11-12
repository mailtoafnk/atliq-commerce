SELECT event_id, session_id, customer_id, event_type, event_ts, event_date, product_id, order_id, device, utm_source
FROM {{ source('silver', 'web_events') }}
