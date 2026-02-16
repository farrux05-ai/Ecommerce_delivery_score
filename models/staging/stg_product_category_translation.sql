select
    trim(product_category_name) as product_category_name,
    nullif(trim(product_category_name_english), '') as product_category_name_english
from {{ source('olist_raw', 'raw_product_category_translation') }}
where nullif(trim(product_category_name), '') is not null
