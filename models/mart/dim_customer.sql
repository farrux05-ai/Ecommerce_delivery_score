select
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix as zip_prefix,
    g.lat as customer_lat,
    g.lng as customer_lng
from {{ref('stg_olist_customers')}} c
left join {{ref('int_dim_geo')}} g 
on c.customer_zip_code_prefix = g.zip_prefix