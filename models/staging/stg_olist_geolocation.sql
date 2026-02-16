select
    trim(geolocation_zip_code_prefix) as geolocation_zip_code_prefix,
    nullif(trim(geolocation_lat), '')::decimal as geolocation_lat,
    nullif(trim(geolocation_lng), '')::decimal as geolocation_lng,
    nullif(trim(geolocation_city), '') as geolocation_city,
    nullif(trim(geolocation_state), '') as geolocation_state
from {{ source('olist_raw', 'raw_olist_geolocation') }}
