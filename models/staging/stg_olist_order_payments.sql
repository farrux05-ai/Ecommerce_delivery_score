select
    trim(order_id) as order_id,
    nullif(trim(payment_sequential), '')::int as payment_sequential,
    nullif(trim(payment_type), '') as payment_type,
    nullif(trim(payment_installments), '')::int as payment_installments,
    nullif(replace(trim(payment_value), ',', ''), '')::numeric as payment_value
from {{ source('olist_raw', 'raw_olist_order_payments') }}
where nullif(trim(order_id), '') is not null
