select
    location_id,
    basket_item_key,
    count(distinct snapshot_date) as observed_snapshot_dates,
    count(distinct product_id) as distinct_product_ids,
    count(distinct package_quantity) as distinct_package_quantities
from {{ ref('mart_cheapest_basket_products') }}
where snapshot_date in (
    date('{{ var("basket_baseline_date") }}'),
    date('{{ var("basket_comparison_date") }}')
)
group by location_id, basket_item_key
having observed_snapshot_dates != 2
    or distinct_product_ids != 1
    or distinct_package_quantities != 1
