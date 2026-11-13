{{
  config(
    partition_by={"field": "comparison_date", "data_type": "date"},
    cluster_by=["city", "item_category", "basket_item_key"]
  )
}}

with baseline as (
    select *
    from {{ ref('fct_standardized_basket_daily') }}
    where snapshot_date = date('{{ var("basket_baseline_date") }}')
      and price_statistic = 'cheapest'
),

comparison as (
    select *
    from {{ ref('fct_standardized_basket_daily') }}
    where snapshot_date = date('{{ var("basket_comparison_date") }}')
      and price_statistic = 'cheapest'
),

movers as (
    select
        concat(baseline.location_id, '|', baseline.basket_item_key) as item_mover_key,
        baseline.location_id,
        baseline.city,
        baseline.state,
        baseline.market_order,
        baseline.basket_item_key,
        baseline.item_name,
        items.item_category,
        baseline.item_order,
        baseline.standard_quantity,
        baseline.standard_unit,
        baseline.snapshot_date as baseline_date,
        comparison.snapshot_date as comparison_date,
        baseline.item_price as baseline_price,
        comparison.item_price as comparison_price,
        comparison.item_price - baseline.item_price as dollar_change,
        100 * safe_divide(
            comparison.item_price - baseline.item_price,
            baseline.item_price
        ) as percent_change
    from baseline
    join comparison using (location_id, basket_item_key)
    join {{ ref('stg_basket_items') }} items using (basket_item_key)
),

with_cross_market_metrics as (
    select
        *,
        percentile_cont(dollar_change, 0.5) over (
            partition by basket_item_key
        ) as cross_market_median_dollar_change,
        percentile_cont(percent_change, 0.5) over (
            partition by basket_item_key
        ) as cross_market_median_percent_change,
        countif(dollar_change > 0) over (
            partition by basket_item_key
        ) as markets_up,
        countif(dollar_change < 0) over (
            partition by basket_item_key
        ) as markets_down,
        countif(dollar_change = 0) over (
            partition by basket_item_key
        ) as markets_flat,
        min(dollar_change) over (
            partition by basket_item_key
        ) as minimum_market_change,
        max(dollar_change) over (
            partition by basket_item_key
        ) as maximum_market_change
    from movers
)

select
    *,
    dense_rank() over (
        partition by city order by dollar_change desc
    ) as city_increase_rank,
    dense_rank() over (
        partition by city order by dollar_change
    ) as city_decrease_rank
from with_cross_market_metrics
