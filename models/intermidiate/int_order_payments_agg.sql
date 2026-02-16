select 
    order_id,
    sum(payment_value) as payment_value_total
from {{ref ('stg_olist_order_payments')}}
group by 1