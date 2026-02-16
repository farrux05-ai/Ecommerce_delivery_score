select
    nullif(trim(order_id), '') as order_id,
    trim(order_item_id) as order_item_id,
    coalesce(nullif(trim(product_id), ''), 'unknown') as product_id,
    coalesce(nullif(trim(seller_id), ''), 'unknown') as seller_id,
    nullif(trim(shipping_limit_date), '')::timestamp as shipping_limit_date,
    nullif(replace(trim(price), ',', ''), '')::numeric as price,
    nullif(replace(trim(freight_value), ',', ''), '')::numeric as freight_value
from {{ source('olist_raw', 'raw_olist_order_items') }}
where trim(order_item_id) is not null
