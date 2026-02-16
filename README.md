# Delivery reliability → Review → Repeat GMV (Olist)

## Business question
**Question:** How much does delivery delay and promised ETA error reduce `review_score` and `repeat GMV`? Which seller/category/state is most affected?

**Decision use-cases**
- Seller policy / penalty / buybox: find drivers of “delay → low review → churn”
- Logistics problem and ETA tuning by region (state/city)
- Packaging/shipping standard proposal by category

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
- `dim_customer`, `dim_seller`, `dim_product`, `dim_geo`

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
dbt test
