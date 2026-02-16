with seller_gmv as (
  select
    seller_id,
    sum(coalesce(price,0)) as gmv_item,
    count(distinct order_id) as orders
  from {{ ref('stg_olist_order_items') }}
  group by 1
),
seller_on_time as (
  select
    oi.seller_id,
    avg(o.is_on_time)::numeric(10,4) as on_time_rate,
    avg(o.avg_review_score)::numeric(10,2) as avg_review_score,
    avg(o.is_repeat_90d)::numeric(10,4) as repeat_rate_90d
  from {{ ref('stg_olist_order_items') }} oi
  join {{ ref('fct_orders') }} o using (order_id)
  group by 1
)
select
  st.seller_id,
  d.seller_state,
  d.seller_city,
  st.on_time_rate,
  st.avg_review_score,
  st.repeat_rate_90d,
  g.gmv_item,
  g.orders
from seller_on_time st
join seller_gmv g using (seller_id)
left join {{ ref('dim_seller') }} d using (seller_id)
order by g.gmv_item desc
