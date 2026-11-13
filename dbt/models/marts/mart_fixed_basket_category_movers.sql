{{
  config(
    partition_by={"field": "comparison_date", "data_type": "date"},
    cluster_by=["city", "item_category"]
  )
}}

with category_totals as (
    select
        concat(location_id, '|', item_category) as category_mover_key,
        location_id,
        city,
        state,
        market_order,
        item_category,
        any_value(baseline_date) as baseline_date,
        any_value(comparison_date) as comparison_date,
        count(*) as item_count,
        sum(baseline_price) as baseline_total,
        sum(comparison_price) as comparison_total,
        sum(dollar_change) as dollar_change
    from {{ ref('mart_fixed_basket_item_movers') }}
    group by location_id, city, state, market_order, item_category
),

with_changes as (
    select
        *,
        100 * safe_divide(dollar_change, baseline_total) as percent_change
    from category_totals
)

select
    *,
    percentile_cont(dollar_change, 0.5) over (
        partition by item_category
    ) as cross_market_median_dollar_change,
    countif(dollar_change > 0) over (
        partition by item_category
    ) as markets_up,
    countif(dollar_change < 0) over (
        partition by item_category
    ) as markets_down,
    countif(dollar_change = 0) over (
        partition by item_category
    ) as markets_flat
from with_changes
