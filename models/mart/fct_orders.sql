with o as (
  select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp as purchase_ts,
    order_delivered_customer_date as delivered_ts,
    order_estimated_delivery_date as estimated_ts
  from {{ ref('stg_olist_orders') }}
),
items as (
  select * from {{ ref('int_order_items_agg') }}
),
pay as (
  select * from {{ ref('int_order_payments_agg') }}
),
rev as (
  select * from {{ ref('int_reviews') }}
),
cust_seq as (
  select
    customer_unique_id,
    order_id,
    first_purchase_ts,
    order_number,
    purchase_ts
  from {{ ref('int_customer_order_sequence') }}
),
enriched as (
  select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchase_ts,
    o.delivered_ts,
    o.estimated_ts,

    /* Delivery reliability */
    case
      when o.delivered_ts is null or o.estimated_ts is null then null
      when o.delivered_ts <= o.estimated_ts then 1 else 0
    end as is_on_time,

    case
      when o.delivered_ts is null or o.estimated_ts is null then null
      else greatest(0, (o.delivered_ts::date - o.estimated_ts::date))  -- delay days (>=0)
    end as delay_days,

    /* Money */
    items.gmv_item,
    items.freight_total,
    items.net_gmv_proxy,
    pay.payment_value_total,

    /* CSAT proxy */
    rev.avg_review_score,

    /* Repeat windows (customer-level) */
    cust_seq.first_purchase_ts,
    cust_seq.order_number,

    case
      when cust_seq.order_number >= 2
       and o.purchase_ts <= cust_seq.first_purchase_ts + interval '30 days'
      then 1 else 0
    end as is_repeat_30d,

    case
      when cust_seq.order_number >= 2
       and o.purchase_ts <= cust_seq.first_purchase_ts + interval '60 days'
      then 1 else 0
    end as is_repeat_60d,

    case
      when cust_seq.order_number >= 2
       and o.purchase_ts <= cust_seq.first_purchase_ts + interval '90 days'
      then 1 else 0
    end as is_repeat_90d

  from o
  left join items on o.order_id = items.order_id
  left join pay   on o.order_id = pay.order_id
  left join rev   on o.order_id = rev.order_id
  left join cust_seq on o.order_id = cust_seq.order_id
)
select * from enriched
