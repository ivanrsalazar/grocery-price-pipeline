{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["location_id", "basket_item_key"]
  )
}}

with comparable_products as (
    select
        candidates.*,
        locations.city,
        locations.state
    from {{ ref('int_comparable_cheapest_items_daily') }} candidates
    join {{ ref('stg_locations') }} locations using (location_id)
    where locations.active
)

select
    concat(
        cast(snapshot_date as string), '|', location_id, '|', basket_item_key
    ) as cheapest_product_key,
    comparable_products.*
from comparable_products
