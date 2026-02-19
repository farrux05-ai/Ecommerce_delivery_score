WITH
/* ---------- Order-level revenue ---------- */
order_rev AS (
  SELECT order_id, SUM(payment_value) AS revenue
  FROM {{ref('stg_olist_order_payments')}}
  GROUP BY 1
),

/* ---------- Order base (order-level, user attached) ---------- */
order_base AS (
  SELECT
    o.order_id,
    c.customer_unique_id,
    c.customer_state,
    DATE_TRUNC('month', o.order_purchase_timestamp)::date AS month,
    o.order_status,
    o.order_delivered_customer_date,
    COALESCE(r.revenue, 0) AS revenue,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END AS is_delivered
  FROM {{ref('stg_olist_orders')}} o
  JOIN {{ref('stg_olist_customers')}} c ON c.customer_id = o.customer_id
  LEFT JOIN order_rev r ON r.order_id = o.order_id
  WHERE o.order_status <> 'canceled'
),

/* ---------- User repeat flag (order_count >= 2) ---------- */
user_counts AS (
  SELECT customer_unique_id, COUNT(DISTINCT order_id) AS orders_cnt
  FROM order_base
  GROUP BY 1
),
user_repeat AS (
  SELECT
    customer_unique_id,
    CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END AS is_repeat_user
  FROM user_counts
),

/* ---------- Delivered rate needs denominator at segment-month level ---------- */
order_enriched AS (
  SELECT
    ob.*,
    ur.is_repeat_user
  FROM order_base ob
  JOIN user_repeat ur USING (customer_unique_id)
),

/* ============================================================
   SEGMENT 1: STATE (order-level, safe)
   ============================================================ */
seg_state AS (
  SELECT
    month,
    'state'::text AS segment_type,
    customer_state::text AS segment_value,
    SUM(revenue) AS revenue,
    COUNT(DISTINCT customer_unique_id) AS buyers,
    AVG(is_repeat_user::float) AS repeat_rate,              -- buyer-level repeat flag averaged over buyers in segment-month
    AVG(is_delivered::float) AS delivered_rate              -- order-level delivered rate
  FROM order_enriched
  GROUP BY 1,2,3
),

/* ============================================================
   SEGMENT 2: PAYMENT TYPE
   - If an order has multiple payment rows/types, we take the
     "dominant" payment_type by highest payment_value.
   ============================================================ */
payment_ranked AS (
  SELECT
    op.order_id,
    op.payment_type,
    op.payment_value,
    ROW_NUMBER() OVER (PARTITION BY op.order_id ORDER BY op.payment_value DESC) AS rn
  FROM {{ref('stg_olist_order_payments')}} op
),
order_payment_type AS (
  SELECT
    order_id,
    payment_type
  FROM payment_ranked
  WHERE rn = 1
),
seg_payment AS (
  SELECT
    oe.month,
    'payment_type'::text AS segment_type,
    opt.payment_type::text AS segment_value,
    SUM(oe.revenue) AS revenue,
    COUNT(DISTINCT oe.customer_unique_id) AS buyers,
    AVG(oe.is_repeat_user::float) AS repeat_rate,
    AVG(oe.is_delivered::float) AS delivered_rate
  FROM order_enriched oe
  JOIN order_payment_type opt ON opt.order_id = oe.order_id
  GROUP BY 1,2,3
),

/* ============================================================
   SEGMENT 3: CATEGORY (revenue allocation by item price share)
   - Allocate order revenue across categories based on item price.
   - Delivered_rate and repeat_rate computed at allocated-row level:
     revenue is allocated, but delivered flag repeats (ok for rate).
   ============================================================ */
items AS (
  SELECT
    oi.order_id,
    oi.product_id,
    oi.price
  FROM {{ref('stg_olist_order_items')}} oi
),
products AS (
  SELECT
    product_id,
    product_category_name
  FROM {{ref('stg_olist_products')}}
),
order_item_cat AS (
  SELECT
    i.order_id,
    COALESCE(p.product_category_name, 'unknown') AS category,
    SUM(i.price) AS cat_item_price
  FROM items i
  LEFT JOIN products p ON p.product_id = i.product_id
  GROUP BY 1,2
),
order_total_item_price AS (
  SELECT
    order_id,
    SUM(cat_item_price) AS total_item_price
  FROM order_item_cat
  GROUP BY 1
),
order_cat_alloc AS (
  SELECT
    oic.order_id,
    oic.category,
    oic.cat_item_price,
    ot.total_item_price,
    CASE WHEN ot.total_item_price = 0 THEN NULL
         ELSE (oic.cat_item_price / ot.total_item_price) END AS price_share
  FROM order_item_cat oic
  JOIN order_total_item_price ot USING (order_id)
),
seg_category AS (
  SELECT
    oe.month,
    'category'::text AS segment_type,
    oca.category::text AS segment_value,
    SUM(oe.revenue * oca.price_share) AS revenue,
    COUNT(DISTINCT oe.customer_unique_id) AS buyers,
    AVG(oe.is_repeat_user::float) AS repeat_rate,
    AVG(oe.is_delivered::float) AS delivered_rate
  FROM order_enriched oe
  JOIN order_cat_alloc oca ON oca.order_id = oe.order_id
  WHERE oca.price_share IS NOT NULL
  GROUP BY 1,2,3
)

SELECT * FROM seg_state
UNION ALL
SELECT * FROM seg_payment
UNION ALL
SELECT * FROM seg_category
ORDER BY month, segment_type, revenue DESC
