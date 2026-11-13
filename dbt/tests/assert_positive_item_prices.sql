select *
from {{ ref('fct_grocery_basket_daily') }}
where item_price <= 0
