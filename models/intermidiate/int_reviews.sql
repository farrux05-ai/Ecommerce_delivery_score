select
    order_id,
    avg(review_score)::numeric(10,2) as avg_review_score
from {{ref('stg_olist_order_reviews')}}
group by order_id