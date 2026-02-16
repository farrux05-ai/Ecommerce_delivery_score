with base as (
  select
    case
      when delay_days is null then 'unknown'
      when delay_days = 0 then '0'
      when delay_days between 1 and 2 then '1-2'
      when delay_days between 3 and 5 then '3-5'
      else '6+'
    end as delay_bucket,
    avg_review_score,
    is_repeat_30d,
    is_repeat_60d,
    is_repeat_90d
  from {{ ref('fct_orders') }}
  where order_status = 'delivered'
    and delivered_ts is not null
    and estimated_ts is not null
)
select
  delay_bucket,
  avg(avg_review_score)::numeric(10,2) as avg_review_score,
  avg(is_repeat_30d)::numeric(10,4) as repeat_rate_30d,
  avg(is_repeat_60d)::numeric(10,4) as repeat_rate_60d,
  avg(is_repeat_90d)::numeric(10,4) as repeat_rate_90d,
  count(*) as orders
from base
-- delivered-only bo‘lgani uchun unknown amalda chiqmaydi
where delay_bucket <> 'unknown'
group by 1
order by
  case delay_bucket
    when '0' then 1
    when '1-2' then 2
    when '3-5' then 3
    when '6+' then 4
    else 5
  end