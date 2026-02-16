select
    trim(order_id) as order_id,
    coalesce(nullif(trim(customer_id), ''), 'unknown') as customer_id,
    coalesce(nullif(trim(order_status), ''), 'unknown') as order_status,
    nullif(trim(order_purchase_timestamp), '')::timestamp as order_purchase_timestamp,
    nullif(trim(order_approved_at), '')::timestamp as order_approved_at,
    nullif(trim(order_delivered_carrier_date), '')::timestamp as order_delivered_carrier_date,
    nullif(trim(order_delivered_customer_date), '')::timestamp as order_delivered_customer_date,
    nullif(trim(order_estimated_delivery_date), '')::timestamp as order_estimated_delivery_date
from {{ source('olist_raw', 'raw_olist_orders') }}
where nullif(trim(order_id), '') is not null
  and nullif(trim(order_purchase_timestamp), '') is not null
