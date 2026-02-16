select
  zip_prefix,
  lat,
  lng
from {{ ref('int_dim_geo') }}
