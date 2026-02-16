# Delivery reliability → Review → Repeat GMV (Olist)

## Business question
**Question:** Yetkazib berish kechikishi va va’da qilingan ETA xatosi `review_score` va `repeat GMV`ni qanchaga tushiryapti? Qaysi seller/category/state’da ta’sir eng katta?

**Decision use-cases**
- Seller policy / penalty / buybox: “delay → low review → churn” driver’larini topish
- Region (state/city) bo‘yicha logistics muammo va ETA tuning
- Category bo‘yicha packaging/shipping standard taklifi

## Stack
- Warehouse: PostgreSQL
- Transform: dbt Core
- BI: Microsoft Power BI

## Data model (high level)
**Lineage:** raw → staging → marts → analysis

### Core facts
- `fct_orders` (grain: order_id)
- `fct_order_items` (grain: order_id + order_item_id)
- `fct_reviews` (grain: order_id)
- `fct_payments` (grain: order_id + payment_seq) or aggregated

### Core dims
- `dim_customer`, `dim_seller`, `dim_product`, `dim_geo` (optional)

## Metrics (semantic layer)
All metric definitions and grain/filters: `docs/metrics.md`

## Key analyses produced
1) **CX impact curve**: delay_days bucket → avg review_score, repeat_rate, repeat_gmv
2) **Seller scorecard**: seller bo‘yicha on-time rate, avg review, repeat, GMV share
3) **Geo lens**: state/city bo‘yicha delay/review/repeat + distance proxy (optional)

## Key findings (Delivery → Review → Repeat)
1) **Delay strongly degrades CSAT:** avg review drops from **4.29 (on-time)** to **1.74 (6+ days late)** (−2.55 pts).
2) **Repeat signal is weaker (order-level proxy):** repeat_90d changes from **2.21% (on-time)** to **2.07% (6+ late)** (−0.14 pp).
3) **Late deliveries are a minority (~6.8%) but high-risk:** targeted fixes can improve CSAT disproportionately.
4) **Seller reliability varies materially among top GMV sellers:** on-time rates range roughly **0.88–0.96** → clear lever for seller policy/SLA.
5) **Non-delivery drivers exist:** some sellers have high on-time but low review (e.g., on-time ~0.90, review ~3.34) → packaging/product quality hypotheses.
6) **Geo hotspots:** RJ and BA show higher delays and lower reviews vs SP; BA also has much higher average distance.


## Dashboard
Power BI report pages:
- Page 1: Executive summary
- Page 2: CX impact curve
- Page 3: Seller scorecard + drill

Screenshots in `/screenshots/`.
Report spec: `dashboard/powerbi/report_spec.md`

## How to run (dbt)
1) Configure `profiles.yml`
2) Run:
```bash
dbt run
dbt test```


---

## 3) `docs/metrics.md` (Semantic layer / Metric contract)
Bu hujjat Power BI’da hamma measure’lar uchun “single source of truth” bo‘ladi.

```md
# Metrics contract (Semantic layer)

## Conventions
- Grain: default grain of the model where metric is defined
- Default filter: unless stated otherwise
- Units: days / currency / percent

---

## On-time rate
- **Definition:** delivered_date ≤ estimated_date bo‘lgan delivered order ulushi
- **Grain:** order_id
- **Model:** fct_orders
- **Filter:** order_status = 'delivered' AND delivered_ts IS NOT NULL AND estimated_ts IS NOT NULL
- **SQL:** avg(is_on_time)

## Delay days
- **Definition:** max(0, delivered_date - estimated_date) (kun)
- **Grain:** order_id
- **Model:** fct_orders
- **Filter:** delivered_ts & estimated_ts not null
- **SQL:** delay_days

## CSAT proxy (Avg review score)
- **Definition:** avg(review_score)
- **Grain:** order_id (aggregated)
- **Model:** fct_orders or fct_reviews
- **Filter:** review_score not null
- **SQL:** avg(avg_review_score) or avg(review_score)

## Repeat rate (30/60/90d)
- **Definition:** first purchase’dan keyin N kunda 2+ order qilgan customer %
- **Grain:** customer_id
- **Model:** (derived from fct_orders + customer order sequence)
- **Filter:** customers with ≥1 order
- **SQL idea:** avg(customer_has_repeat_Nd)

## Repeat GMV
- **Definition:** repeat orderlar bo‘yicha item price sum
- **Grain:** order_id (repeat orders only) or customer_id (cohort-level)
- **Model:** fct_order_items + repeat flags
- **SQL:** sum(price) where is_repeat_Nd = 1

## Net GMV proxy
- **Definition:** GMV − freight
- **Grain:** order_id or item-level
- **Model:** fct_orders / fct_order_items
- **SQL:** sum(price) - sum(freight_value)

