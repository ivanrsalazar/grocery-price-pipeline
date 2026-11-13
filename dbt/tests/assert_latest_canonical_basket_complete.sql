with latest_date as (
    select max(snapshot_date) as snapshot_date
    from {{ ref('mart_grocery_basket_daily') }}
    where basket_id = 'canonical_essentials'
)

select baskets.*
from {{ ref('mart_grocery_basket_daily') }} baskets
join latest_date using (snapshot_date)
where baskets.basket_id = 'canonical_essentials'
  and not baskets.is_complete
