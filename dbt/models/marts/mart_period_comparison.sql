with latest_common_date as (
    select basket_id, max(snapshot_date) as comparison_date
    from (
        select basket_id, snapshot_date, countif(is_complete) as complete_markets
        from {{ ref('mart_grocery_basket_daily') }}
        group by basket_id, snapshot_date
    )
    where complete_markets = (select countif(active) from {{ ref('stg_locations') }})
    group by basket_id
),

baseline as (
    select *
    from {{ ref('mart_grocery_basket_daily') }}
    where snapshot_date = date('{{ var("basket_baseline_date") }}')
      and is_complete
),

comparison as (
    select baskets.*
    from {{ ref('mart_grocery_basket_daily') }} baskets
    join latest_common_date
      on baskets.basket_id = latest_common_date.basket_id
     and baskets.snapshot_date = latest_common_date.comparison_date
    where baskets.is_complete
)

select
    comparison.basket_id,
    comparison.location_id,
    comparison.city,
    baseline.snapshot_date as baseline_date,
    comparison.snapshot_date as comparison_date,
    baseline.basket_total as baseline_total,
    comparison.basket_total as comparison_total,
    comparison.basket_total - baseline.basket_total as total_change,
    100 * safe_divide(
        comparison.basket_total - baseline.basket_total,
        baseline.basket_total
    ) as percent_change
from comparison
join baseline using (basket_id, location_id)
