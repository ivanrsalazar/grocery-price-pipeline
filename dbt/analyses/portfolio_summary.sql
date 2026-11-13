select
    basket_id,
    city,
    baseline_date,
    comparison_date,
    round(baseline_total, 2) as baseline_total,
    round(comparison_total, 2) as comparison_total,
    round(percent_change, 1) as percent_change
from {{ ref('mart_period_comparison') }}
order by basket_id, comparison_total
