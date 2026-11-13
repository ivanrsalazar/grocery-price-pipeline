select
    products.snapshot_date,
    products.location_id,
    basket.basket_id,
    basket.basket_item_key,
    basket.item_name,
    basket.search_term,
    basket.item_order,
    basket.standard_quantity,
    basket.standard_unit,
    products.product_id,
    products.upc,
    products.brand,
    products.description,
    products.size,
    products.price as shelf_price,
    products.normalized_quantity as package_quantity,
    products.unit_price,
    products.unit_price * basket.standard_quantity as standardized_price
from {{ ref('int_unit_prices') }} products
join {{ ref('stg_basket_items') }} basket
  on products.search_term = basket.search_term
 and basket.basket_id = 'canonical_essentials'
where products.normalized_unit = basket.standard_unit
  and products.normalized_quantity > 0
  and products.unit_price > 0
  and not products.is_ambiguous_multipack
  and not (
      basket.search_term = 'orange juice'
      and regexp_contains(lower(products.description), r'\bdrink\b')
  )
  and not (
      basket.search_term = 'butter'
      and regexp_contains(
          lower(products.description),
          r'\b(?:spreadable|spread|margarine)\b|with (?:olive|canola) oil|plant.?based'
      )
  )
