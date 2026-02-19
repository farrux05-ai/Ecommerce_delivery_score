WITH
order_rev AS (
  SELECT order_id, SUM(payment_value) AS revenue
  FROM {{ ref('stg_olist_order_payments')}}
  GROUP BY 1
),
order_base AS (
  SELECT
    o.order_id,
    c.customer_unique_id,
    DATE(o.order_purchase_timestamp) AS date,
    o.order_status,
    COALESCE(r.revenue, 0) AS revenue
  FROM {{ref('stg_olist_orders')}} o
  JOIN {{ref('stg_olist_customers')}} c ON c.customer_id = o.customer_id
  LEFT JOIN order_rev r ON r.order_id = o.order_id
  WHERE o.order_status <> 'canceled'
),
tagged AS (
  SELECT
    ob.*,
    MIN(date) OVER (PARTITION BY customer_unique_id) AS first_order_date
  FROM order_base ob
)
SELECT
  date,
  SUM(revenue) AS revenue,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_unique_id) AS buyers,
  COUNT(DISTINCT CASE WHEN date = first_order_date THEN customer_unique_id END) AS new_buyers,
  COUNT(DISTINCT CASE WHEN date > first_order_date THEN customer_unique_id END) AS returning_buyers,
  CASE WHEN COUNT(DISTINCT order_id) = 0 THEN NULL
       ELSE SUM(revenue) / COUNT(DISTINCT order_id) END AS aov,
  CASE WHEN COUNT(DISTINCT customer_unique_id) = 0 THEN NULL
       ELSE SUM(revenue) / COUNT(DISTINCT customer_unique_id) END AS arppb
FROM tagged
GROUP BY 1
ORDER BY 1
