with market_median_items as (
    select
        prices.snapshot_date,
        prices.location_id,
        basket.basket_id,
        basket.basket_item_key,
        basket.item_name,
        basket.search_term,
        basket.item_order,
        basket.quantity,
        basket.pricing_method,
        cast(null as string) as upc,
        prices.median_price as item_price,
        prices.product_count as candidate_product_count
    from {{ ref('int_item_market_daily') }} prices
    join {{ ref('stg_basket_items') }} basket
      on prices.search_term = basket.search_term
     and basket.pricing_method = 'market_median'
),

matched_sku_items as (
    select
        products.snapshot_date,
        products.location_id,
        basket.basket_id,
        basket.basket_item_key,
        basket.item_name,
        basket.search_term,
        basket.item_order,
        basket.quantity,
        basket.pricing_method,
        basket.upc,
        approx_quantiles(products.price, 2)[offset(1)] as item_price,
        count(distinct products.product_id) as candidate_product_count
    from {{ ref('stg_kroger_products') }} products
    join {{ ref('stg_basket_items') }} basket
      on products.search_term = basket.search_term
     and products.upc = basket.upc
     and basket.pricing_method = 'matched_sku'
    where products.price > 0
    group by
        products.snapshot_date,
        products.location_id,
        basket.basket_id,
        basket.basket_item_key,
        basket.item_name,
        basket.search_term,
        basket.item_order,
        basket.quantity,
        basket.pricing_method,
        basket.upc
)

select * from market_median_items
union all
select * from matched_sku_items
