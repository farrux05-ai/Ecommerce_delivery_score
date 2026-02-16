with base as (
  select
    c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp as purchase_ts
  from {{ ref('stg_olist_orders') }} o
  join {{ ref('stg_olist_customers') }} c
    on o.customer_id = c.customer_id
  where c.customer_unique_id is not null
    and o.order_purchase_timestamp is not null
),
sequenced as (
  select
    *,
    min(purchase_ts) over (partition by customer_unique_id) as first_purchase_ts,
    row_number() over (
      partition by customer_unique_id
      order by purchase_ts, order_id
    ) as order_number
  from base
)
select
  customer_unique_id,
  order_id,
  purchase_ts,
  first_purchase_ts,
  order_number
from sequenced
