select
    trim(review_id) as review_id,
    trim(order_id) as order_id,
    nullif(trim(review_score), '')::int as review_score,
    nullif(trim(review_comment_title), '') as review_comment_title,
    nullif(trim(review_comment_message), '') as review_comment_message,
    nullif(trim(review_creation_date), '')::timestamp as review_creation_date,
    nullif(trim(review_answer_timestamp), '')::timestamp as review_answer_timestamp
from {{ source('olist_raw', 'raw_olist_order_reviews') }}
where nullif(trim(review_id), '') is not null
  and nullif(trim(order_id), '') is not null
