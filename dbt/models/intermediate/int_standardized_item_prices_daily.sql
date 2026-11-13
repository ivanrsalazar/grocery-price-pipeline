with medians as (
    select
        snapshot_date,
        location_id,
        basket_id,
        basket_item_key,
        item_name,
        search_term,
        item_order,
        standard_quantity,
        standard_unit,
        approx_quantiles(standardized_price, 2)[offset(1)] as item_price,
        count(distinct product_id) as candidate_product_count
    from {{ ref('int_standardized_item_candidates_daily') }}
    group by
        snapshot_date, location_id, basket_id, basket_item_key, item_name,
        search_term, item_order, standard_quantity, standard_unit
)

select
    medians.*,
    'median' as price_statistic
from medians

union all

select
    snapshot_date,
    location_id,
    basket_id,
    basket_item_key,
    item_name,
    search_term,
    item_order,
    standard_quantity,
    standard_unit,
    standardized_price as item_price,
    comparable_candidate_count as candidate_product_count,
    'cheapest' as price_statistic
from {{ ref('int_comparable_cheapest_items_daily') }}
