# Power BI Report Spec — Delivery reliability → Review → Repeat GMV

## Dataset / tables used
Preferred: use dbt **analysis outputs** for performance and simplicity:
- cx_impact_curve
- seller_scorecard
- geo_lens
Plus a small set of dimensions for slicing (state/category).

## Global slicers
- Date range (purchase date)
- Customer state
- Product category
- Seller (optional)

## Page 1 — Executive summary
**KPIs**
- On-time rate (delivered)
- Avg delay days
- Avg review score
- Repeat rate 90d
- Net GMV proxy

**Charts**
- Trend: on-time rate over time (weekly/monthly)
- Trend: repeat rate over time

## Page 2 — CX impact curve
**Charts**
- Column: delay_bucket → avg_review_score
- Column: delay_bucket → repeat_rate_90d
- Table: delay_bucket metrics + order count

**Notes**
- Ensure bucket ordering: 0, 1–2, 3–5, 6+, unknown
- Add tooltip: order volume + net GMV

## Page 3 — Seller scorecard
**Table**
- seller_id, GMV, on_time_rate, avg_review_score, repeat_rate_90d
- Conditional formatting: highlight low on-time & low review

**Scatter**
- X: on_time_rate
- Y: avg_review_score
- Size: GMV
- Filter: Top sellers by GMV

## Export / sharing
- Provide screenshots in `/screenshots/`
- Include short “how to read” bullets in README
