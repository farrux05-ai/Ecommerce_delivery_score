WITH base AS (
  SELECT
    order_id,
    DATE(order_purchase_timestamp) AS date,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_status
  FROM {{ref('stg_olist_orders')}}
  WHERE order_status <> 'canceled'
),
durations AS (
  SELECT
    date,
    order_id,
    (order_approved_at IS NOT NULL) AS is_approved,
    (order_delivered_carrier_date IS NOT NULL) AS is_shipped,
    (order_delivered_customer_date IS NOT NULL) AS is_delivered,
    CASE
      WHEN order_purchase_timestamp IS NOT NULL AND order_approved_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (order_approved_at - order_purchase_timestamp)) / 3600.0
      ELSE NULL
    END AS purchase_to_approved_hours,
    CASE
      WHEN order_approved_at IS NOT NULL AND order_delivered_carrier_date IS NOT NULL
        THEN EXTRACT(EPOCH FROM (order_delivered_carrier_date - order_approved_at)) / 3600.0
      ELSE NULL
    END AS approved_to_shipped_hours,
    CASE
      WHEN order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date IS NOT NULL
        THEN EXTRACT(EPOCH FROM (order_delivered_customer_date - order_delivered_carrier_date)) / 3600.0
      ELSE NULL
    END AS shipped_to_delivered_hours,
    CASE
      WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
        THEN (order_delivered_customer_date::date > order_estimated_delivery_date::date)
      ELSE NULL
    END AS is_late
  FROM base
)
SELECT
  date,
  COUNT(*) AS purchased,
  COUNT(*) FILTER (WHERE is_approved) AS approved,
  COUNT(*) FILTER (WHERE is_shipped) AS shipped,
  COUNT(*) FILTER (WHERE is_delivered) AS delivered,
  CASE WHEN COUNT(*) = 0 THEN NULL
       ELSE 1.0 * COUNT(*) FILTER (WHERE is_delivered) / COUNT(*) END AS delivered_rate,
  AVG(purchase_to_approved_hours) AS avg_purchase_to_approved_hours,
  AVG(approved_to_shipped_hours) AS avg_approved_to_shipped_hours,
  AVG(shipped_to_delivered_hours) AS avg_shipped_to_delivered_hours,
  AVG(CASE WHEN is_late IS TRUE THEN 1.0
           WHEN is_late IS FALSE THEN 0.0
           ELSE NULL END) AS late_rate
FROM durations
GROUP BY 1
ORDER BY 1