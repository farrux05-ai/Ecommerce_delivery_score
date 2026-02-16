
select
  geolocation_zip_code_prefix as zip_prefix,
  avg(geolocation_lat) as lat,
  avg(geolocation_lng) as lng
from {{ ref('stg_olist_geolocation') }}
where geolocation_lat is not null
  and geolocation_lng is not null
group by 1
