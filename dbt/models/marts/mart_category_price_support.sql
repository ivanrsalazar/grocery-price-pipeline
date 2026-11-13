{{
  config(
    partition_by={"field": "comparison_date", "data_type": "date"},
    cluster_by=["city", "grocery_category", "search_term"]
  )
}}

with baseline as (
    select
        location_id,
        bucket_key,
        search_term,
        normalized_unit,
        product_id,
        upc,
        brand,
        description,
        size,
        price as shelf_price,
        unit_price
    from {{ ref('int_unit_prices') }}
    where snapshot_date = date('{{ var("basket_baseline_date") }}')
      and normalized_unit is not null
      and normalized_quantity > 0
      and unit_price > 0
      and not is_ambiguous_multipack
),

comparison as (
    select
        location_id,
        bucket_key,
        search_term,
        normalized_unit,
        product_id,
        upc,
        brand,
        description,
        size,
        price as shelf_price,
        unit_price
    from {{ ref('int_unit_prices') }}
    where snapshot_date = date('{{ var("basket_comparison_date") }}')
      and normalized_unit is not null
      and normalized_quantity > 0
      and unit_price > 0
      and not is_ambiguous_multipack
),

matched as (
    select
        baseline.location_id,
        baseline.bucket_key,
        baseline.search_term,
        baseline.normalized_unit,
        baseline.product_id,
        baseline.upc as baseline_upc,
        baseline.brand as baseline_brand,
        baseline.description as baseline_description,
        baseline.size as baseline_size,
        baseline.shelf_price as baseline_shelf_price,
        baseline.unit_price as baseline_unit_price,
        comparison.upc as comparison_upc,
        comparison.brand as comparison_brand,
        comparison.description as comparison_description,
        comparison.size as comparison_size,
        comparison.shelf_price as comparison_shelf_price,
        comparison.unit_price as comparison_unit_price,
        case
            when baseline.bucket_key = 'produce' then 'Produce'
            when baseline.bucket_key = 'dairy' then 'Dairy and eggs'
            when baseline.bucket_key = 'bakery' then 'Bakery'
            when baseline.bucket_key = 'meat_seafood' then 'Meat and seafood'
            when baseline.bucket_key = 'beverages' then 'Beverages'
            when baseline.bucket_key = 'frozen' then 'Frozen'
            when baseline.bucket_key in ('snacks', 'candy') then 'Snacks and candy'
            when baseline.bucket_key in (
                'canned_packaged',
                'breakfast',
                'sauces',
                'condiment_sauces'
            ) then 'Pantry'
        end as grocery_category
    from baseline
    join comparison using (
        location_id,
        bucket_key,
        search_term,
        normalized_unit,
        product_id
    )
)

select
    to_hex(md5(concat(
        matched.location_id, '|',
        matched.grocery_category, '|',
        matched.search_term, '|',
        matched.normalized_unit, '|',
        matched.product_id
    ))) as category_support_key,
    matched.location_id,
    locations.city,
    locations.state,
    locations.market_order,
    matched.grocery_category,
    matched.search_term,
    matched.normalized_unit,
    matched.product_id as sku,
    date('{{ var("basket_baseline_date") }}') as baseline_date,
    date('{{ var("basket_comparison_date") }}') as comparison_date,
    matched.* except (
        location_id,
        bucket_key,
        grocery_category,
        search_term,
        normalized_unit,
        product_id
    ),
    100 * safe_divide(
        matched.comparison_unit_price - matched.baseline_unit_price,
        matched.baseline_unit_price
    ) as sku_unit_price_percent_change
from matched
join {{ ref('stg_locations') }} locations using (location_id)
where matched.grocery_category is not null
