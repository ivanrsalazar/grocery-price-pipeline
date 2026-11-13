with latest_complete_date as (
    select basket_id, max(snapshot_date) as snapshot_date
    from {{ ref('mart_grocery_basket_daily') }}
    where is_complete
    group by basket_id
),

latest as (
    select baskets.*
    from {{ ref('mart_grocery_basket_daily') }} baskets
    join latest_complete_date using (basket_id, snapshot_date)
    where baskets.is_complete
)

select
    *,
    rank() over (partition by basket_id order by basket_total) as affordability_rank,
    basket_total - min(basket_total) over (partition by basket_id) as premium_over_cheapest,
    100 * safe_divide(
        basket_total - min(basket_total) over (partition by basket_id),
        min(basket_total) over (partition by basket_id)
    ) as premium_over_cheapest_pct
from latest
