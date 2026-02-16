select
    trim(customer_id) as customer_id,
    coalesce(nullif(trim(customer_unique_id), ''), 'unknown') as customer_unique_id,
    nullif(trim(customer_zip_code_prefix), '') as customer_zip_code_prefix,
    nullif(trim(customer_city), '') as customer_city,
    nullif(trim(customer_state), '') as customer_state
from {{ source('olist_raw', 'raw_olist_customers') }}
where nullif(trim(customer_id), '') is not null
