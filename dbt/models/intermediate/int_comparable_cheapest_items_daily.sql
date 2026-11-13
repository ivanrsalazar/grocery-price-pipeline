with paired_candidates as (
    select
        baseline.location_id,
        baseline.basket_item_key,
        baseline.product_id,
        baseline.package_quantity,
        baseline.standardized_price as baseline_standardized_price,
        comparison.standardized_price as comparison_standardized_price,
        count(*) over (
            partition by baseline.location_id, baseline.basket_item_key
        ) as comparable_candidate_count,
        row_number() over (
            partition by baseline.location_id, baseline.basket_item_key
            order by
                baseline.standardized_price,
                comparison.standardized_price,
                baseline.product_id
        ) as comparable_price_rank
    from {{ ref('int_standardized_item_candidates_daily') }} baseline
    join {{ ref('int_standardized_item_candidates_daily') }} comparison
      on baseline.location_id = comparison.location_id
     and baseline.basket_item_key = comparison.basket_item_key
     and baseline.product_id = comparison.product_id
     and baseline.package_quantity = comparison.package_quantity
    where baseline.snapshot_date = date('{{ var("basket_baseline_date") }}')
      and comparison.snapshot_date = date('{{ var("basket_comparison_date") }}')
),

selected_products as (
    select * except (comparable_price_rank)
    from paired_candidates
    where comparable_price_rank = 1
)

select
    candidates.*,
    selected.comparable_candidate_count,
    selected.baseline_standardized_price,
    selected.comparison_standardized_price
from {{ ref('int_standardized_item_candidates_daily') }} candidates
join selected_products selected
  on candidates.location_id = selected.location_id
 and candidates.basket_item_key = selected.basket_item_key
 and candidates.product_id = selected.product_id
 and candidates.package_quantity = selected.package_quantity
