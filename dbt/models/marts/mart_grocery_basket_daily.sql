{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["basket_id", "location_id"]
  )
}}

with expected as (
    select basket_id, count(*) as expected_item_count
    from {{ ref('stg_basket_items') }}
    group by basket_id
),

totals as (
    select
        snapshot_date,
        location_id,
        any_value(city) as city,
        any_value(state) as state,
        any_value(market_order) as market_order,
        basket_id,
        count(*) as observed_item_count,
        sum(extended_price) as basket_total
    from {{ ref('fct_grocery_basket_daily') }}
    group by snapshot_date, location_id, basket_id
)

select
    concat(cast(totals.snapshot_date as string), '|', totals.location_id, '|', totals.basket_id) as basket_daily_key,
    totals.*,
    expected.expected_item_count,
    safe_divide(totals.observed_item_count, expected.expected_item_count) as coverage_ratio,
    totals.observed_item_count = expected.expected_item_count as is_complete
from totals
join expected using (basket_id)
