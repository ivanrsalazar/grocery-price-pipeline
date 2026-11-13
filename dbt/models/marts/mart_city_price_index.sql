{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["basket_id", "location_id"]
  )
}}

with complete_baskets as (
    select *
    from {{ ref('mart_grocery_basket_daily') }}
    where is_complete
),

indexed as (
    select
        *,
        first_value(snapshot_date) over (
            partition by basket_id, location_id order by snapshot_date
        ) as baseline_date,
        first_value(basket_total) over (
            partition by basket_id, location_id order by snapshot_date
        ) as baseline_total
    from complete_baskets
)

select
    *,
    100 * safe_divide(basket_total, baseline_total) as price_index,
    100 * safe_divide(basket_total - baseline_total, baseline_total) as percent_change_from_baseline
from indexed
