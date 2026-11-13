{{
  config(
    partition_by={"field": "comparison_date", "data_type": "date"},
    cluster_by=["city", "grocery_category"]
  )
}}

with baseline as (
    select
        location_id,
        bucket_key,
        search_term,
        normalized_unit,
        product_id,
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
        unit_price
    from {{ ref('int_unit_prices') }}
    where snapshot_date = date('{{ var("basket_comparison_date") }}')
      and normalized_unit is not null
      and normalized_quantity > 0
      and unit_price > 0
      and not is_ambiguous_multipack
),

matched_skus as (
    select
        baseline.location_id,
        baseline.bucket_key,
        baseline.search_term,
        baseline.product_id,
        comparison.unit_price / baseline.unit_price as price_relative
    from baseline
    join comparison using (
        location_id,
        bucket_key,
        search_term,
        normalized_unit,
        product_id
    )
),

term_indexes as (
    select
        location_id,
        bucket_key,
        search_term,
        count(*) as matched_sku_count,
        exp(avg(ln(price_relative))) as term_price_relative
    from matched_skus
    group by location_id, bucket_key, search_term
),

categorized_terms as (
    select
        location_id,
        search_term,
        matched_sku_count,
        term_price_relative,
        case
            when bucket_key = 'produce' then 'Produce'
            when bucket_key = 'dairy' then 'Dairy and eggs'
            when bucket_key = 'bakery' then 'Bakery'
            when bucket_key = 'meat_seafood' then 'Meat and seafood'
            when bucket_key = 'beverages' then 'Beverages'
            when bucket_key = 'frozen' then 'Frozen'
            when bucket_key in ('snacks', 'candy') then 'Snacks and candy'
            when bucket_key in (
                'canned_packaged',
                'breakfast',
                'sauces',
                'condiment_sauces'
            ) then 'Pantry'
        end as grocery_category
    from term_indexes
),

category_indexes as (
    select
        location_id,
        grocery_category,
        count(*) as search_term_count,
        sum(matched_sku_count) as supporting_product_count,
        100 * (exp(avg(ln(term_price_relative))) - 1) as category_percent_change
    from categorized_terms
    where grocery_category is not null
    group by location_id, grocery_category
),

with_market_details as (
    select
        concat(category.location_id, '|', category.grocery_category) as category_mover_key,
        category.location_id,
        locations.city,
        locations.state,
        locations.market_order,
        category.grocery_category,
        date('{{ var("basket_baseline_date") }}') as baseline_date,
        date('{{ var("basket_comparison_date") }}') as comparison_date,
        category.search_term_count,
        category.supporting_product_count,
        category.category_percent_change
    from category_indexes category
    join {{ ref('stg_locations') }} locations using (location_id)
)

select
    *,
    percentile_cont(category_percent_change, 0.5) over (
        partition by grocery_category
    ) as cross_market_median_percent_change,
    countif(category_percent_change > 0.05) over (
        partition by grocery_category
    ) as markets_up,
    countif(category_percent_change < -0.05) over (
        partition by grocery_category
    ) as markets_down,
    countif(abs(category_percent_change) <= 0.05) over (
        partition by grocery_category
    ) as markets_flat
from with_market_details
