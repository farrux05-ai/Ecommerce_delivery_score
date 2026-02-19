WITH user_orders AS (
  SELECT
    c.customer_unique_id,
    DATE_TRUNC('month', o.order_purchase_timestamp)::date AS order_month
  FROM {{ref('stg_olist_orders')}} o
  JOIN {{ref('stg_olist_customers')}} c ON c.customer_id = o.customer_id
  WHERE o.order_status <> 'canceled'
),
cohort AS (
  SELECT
    customer_unique_id,
    MIN(order_month) AS cohort_month
  FROM user_orders
  GROUP BY 1
),
activity AS (
  SELECT
    u.customer_unique_id,
    c.cohort_month,
    u.order_month,
    ((DATE_PART('year', u.order_month) - DATE_PART('year', c.cohort_month)) * 12
     + (DATE_PART('month', u.order_month) - DATE_PART('month', c.cohort_month)))::int AS month_index
  FROM user_orders u
  JOIN cohort c USING (customer_unique_id)
),
SELECT
  r.cohort_month,
  r.month_index,
  cs.cohort_size,
  r.retained_users,
  CASE WHEN cs.cohort_size = 0 THEN NULL
       ELSE 1.0 * r.retained_users / cs.cohort_size END AS retention_rate
FROM retained r
JOIN cohort_size cs USING (cohort_month)
ORDER BY 1,2