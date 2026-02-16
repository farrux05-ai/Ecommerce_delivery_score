with base as (
  select
    o.order_id,
    c.customer_state,
    s.seller_state,
    o.delay_days,
    o.avg_review_score,
    c.customer_lat, c.customer_lng,
    s.seller_lat, s.seller_lng
  from {{ ref('fct_orders') }} o
  left join {{ ref('dim_customer') }} c using (customer_id)
  left join (
    select distinct order_id, seller_id
    from {{ ref('stg_olist_order_items') }}
  ) oi using (order_id)
  left join {{ ref('dim_seller') }} s using (seller_id)
),
dist as (
  select
    *,
    case
      when customer_lat is null or customer_lng is null or seller_lat is null or seller_lng is null then null
      else
        2 * 6371 * asin(
          sqrt(
            power(sin(radians((customer_lat - seller_lat) / 2)), 2) +
            cos(radians(seller_lat)) * cos(radians(customer_lat)) *
            power(sin(radians((customer_lng - seller_lng) / 2)), 2)
          )
        )
    end as distance_km
  from base
)
select
  customer_state,
  avg(delay_days)::numeric(10,2) as avg_delay_days,
  avg(avg_review_score)::numeric(10,2) as avg_review_score,
  avg(distance_km)::numeric(10,2) as avg_distance_km,
  count(*) as orders
from dist
group by 1
order by orders desc
