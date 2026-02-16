# Metrics contract (Semantic layer)

## Conventions
- Grain: default grain of the model where metric is defined
- Default filter: unless stated otherwise
- Units: days / currency / percent

---

## On-time rate
- **Definition:** delivered_date ≤ estimated_date bo‘lgan delivered order ulushi
- **Grain:** order_id
- **Model:** fct_orders
- **Filter:** order_status = 'delivered' AND delivered_ts IS NOT NULL AND estimated_ts IS NOT NULL
- **SQL:** avg(is_on_time)

## Delay days
- **Definition:** max(0, delivered_date - estimated_date) (kun)
- **Grain:** order_id
- **Model:** fct_orders
- **Filter:** delivered_ts & estimated_ts not null
- **SQL:** delay_days

## CSAT proxy (Avg review score)
- **Definition:** avg(review_score)
- **Grain:** order_id (aggregated)
- **Model:** fct_orders or fct_reviews
- **Filter:** review_score not null
- **SQL:** avg(avg_review_score) or avg(review_score)

## Repeat rate (30/60/90d)
- **Definition:** first purchase’dan keyin N kunda 2+ order qilgan customer %
- **Grain:** customer_id
- **Model:** (derived from fct_orders + customer order sequence)
- **Filter:** customers with ≥1 order
- **SQL idea:** avg(customer_has_repeat_Nd)

## Repeat GMV
- **Definition:** repeat orderlar bo‘yicha item price sum
- **Grain:** order_id (repeat orders only) or customer_id (cohort-level)
- **Model:** fct_order_items + repeat flags
- **SQL:** sum(price) where is_repeat_Nd = 1

## Net GMV proxy
- **Definition:** GMV − freight
- **Grain:** order_id or item-level
- **Model:** fct_orders / fct_order_items
- **SQL:** sum(price) - sum(freight_value)

