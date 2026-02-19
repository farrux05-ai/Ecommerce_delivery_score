WITH user_orders AS (
  SELECT
    c.customer_unique_id,
    DATE(o.order_purchase_timestamp) AS order_date
  FROM {{ref('stg_olist_orders')}} o
  JOIN {{ref('stg_olist_customers')}} c ON c.customer_id = o.customer_id
  WHERE o.order_status <> 'canceled'
),
ranked AS (
  SELECT
    customer_unique_id,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_date) AS rn
  FROM user_orders
),
first_second AS (
  SELECT
    customer_unique_id,
    MAX(CASE WHEN rn = 1 THEN order_date END) AS first_date,
    MAX(CASE WHEN rn = 2 THEN order_date END) AS second_date
  FROM ranked
  GROUP BY 1
),
counts AS (
  SELECT
    customer_unique_id,
    COUNT(*) AS orders_cnt
  FROM user_orders
  GROUP BY 1
),
repeat_users AS (
  SELECT
    fs.customer_unique_id,
    (fs.second_date - fs.first_date) AS days_to_2nd
  FROM first_second fs
  WHERE fs.second_date IS NOT NULL
)
SELECT
  AVG(CASE WHEN c.orders_cnt >= 2 THEN 1.0 ELSE 0.0 END) AS repeat_rate,
  AVG(r.days_to_2nd) AS avg_days_to_2nd,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.days_to_2nd) AS median_days_to_2nd,
  AVG(c.orders_cnt) AS orders_per_buyer
FROM counts c
LEFT JOIN repeat_users r
  ON r.customer_unique_id = c.customer_unique_id
