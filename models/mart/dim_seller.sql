select
  s.seller_id,
  s.seller_city,
  s.seller_state,
  s.seller_zip_code_prefix as zip_prefix,
  g.lat as seller_lat,
  g.lng as seller_lng
from {{ ref('stg_olist_sellers') }} s
left join {{ ref('int_dim_geo') }} g
  on s.seller_zip_code_prefix = g.zip_prefix
