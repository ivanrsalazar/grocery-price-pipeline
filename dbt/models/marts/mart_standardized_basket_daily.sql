{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["price_statistic", "location_id"]
  )
}}

with expected as (
    select count(*) as expected_item_count
    from {{ ref('stg_basket_items') }}
    where basket_id = 'canonical_essentials'
),

totals as (
    select
        snapshot_date,
        location_id,
        any_value(city) as city,
        any_value(state) as state,
        any_value(market_order) as market_order,
        basket_id,
        price_statistic,
        count(*) as observed_item_count,
        sum(item_price) as basket_total,
        min(candidate_product_count) as minimum_item_candidates
    from {{ ref('fct_standardized_basket_daily') }}
    group by snapshot_date, location_id, basket_id, price_statistic
)

select
    concat(
        cast(totals.snapshot_date as string), '|', totals.location_id, '|',
        totals.basket_id, '|', totals.price_statistic
    ) as standardized_basket_daily_key,
    totals.*,
    expected.expected_item_count,
    safe_divide(totals.observed_item_count, expected.expected_item_count) as coverage_ratio,
    totals.observed_item_count = expected.expected_item_count as is_complete
from totals
cross join expected
