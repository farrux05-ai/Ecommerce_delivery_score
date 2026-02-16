select
    trim(seller_id) as seller_id,
    nullif(trim(seller_zip_code_prefix), '') as seller_zip_code_prefix,
    nullif(trim(seller_city), '') as seller_city,
    nullif(trim(seller_state), '') as seller_state
from {{ source('olist_raw', 'raw_olist_sellers') }}
where nullif(trim(seller_id), '') is not null
