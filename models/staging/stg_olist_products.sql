select
    trim(product_id) as product_id,
    nullif(trim(product_category_name), '') as product_category_name,

    nullif(trim(product_name_lenght), '')::int as product_name_lenght,
    nullif(trim(product_description_lenght), '')::int as product_description_lenght,
    nullif(trim(product_photos_qty), '')::int as product_photos_qty,

    nullif(replace(trim(product_weight_g), ',', ''), '')::numeric as product_weight_g,
    nullif(replace(trim(product_length_cm), ',', ''), '')::numeric as product_length_cm,
    nullif(replace(trim(product_height_cm), ',', ''), '')::numeric as product_height_cm,
    nullif(replace(trim(product_width_cm), ',', ''), '')::numeric as product_width_cm
from {{ source('olist_raw', 'raw_olist_products') }}
where nullif(trim(product_id), '') is not null
