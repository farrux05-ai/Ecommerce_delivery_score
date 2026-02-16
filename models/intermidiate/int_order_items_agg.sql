select
  order_id,
  sum(coalesce(price,0)) as gmv_item,
  sum(coalesce(freight_value,0)) as freight_total,
  sum(coalesce(price,0)) - sum(coalesce(freight_value,0)) as net_gmv_proxy
from {{ ref('stg_olist_order_items') }}
group by 1
