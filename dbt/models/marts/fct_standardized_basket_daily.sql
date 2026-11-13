{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["price_statistic", "location_id", "basket_item_key"]
  )
}}

select
    concat(
        cast(items.snapshot_date as string), '|', items.location_id, '|',
        items.basket_item_key, '|', items.price_statistic
    ) as standardized_observation_key,
    items.snapshot_date,
    items.location_id,
    locations.city,
    locations.state,
    locations.market_order,
    items.basket_id,
    items.basket_item_key,
    items.item_name,
    items.search_term,
    items.item_order,
    items.standard_quantity,
    items.standard_unit,
    items.price_statistic,
    items.item_price,
    items.candidate_product_count
from {{ ref('int_standardized_item_prices_daily') }} items
join {{ ref('stg_locations') }} locations using (location_id)
where locations.active
