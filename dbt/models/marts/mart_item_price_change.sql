{{
  config(
    partition_by={"field": "comparison_date", "data_type": "date"},
    cluster_by=["basket_id", "location_id", "basket_item_key"]
  )
}}

with bounds as (
    select
        basket_id,
        basket_item_key,
        location_id,
        min(snapshot_date) as baseline_date,
        max(snapshot_date) as comparison_date
    from {{ ref('fct_grocery_basket_daily') }}
    group by basket_id, basket_item_key, location_id
)

select
    bounds.basket_id,
    bounds.basket_item_key,
    latest_observation.item_name,
    latest_observation.search_term,
    bounds.location_id,
    latest_observation.city,
    bounds.baseline_date,
    bounds.comparison_date,
    baseline.item_price as baseline_price,
    latest_observation.item_price as current_price,
    latest_observation.item_price - baseline.item_price as price_change,
    100 * safe_divide(latest_observation.item_price - baseline.item_price, baseline.item_price) as percent_change
from bounds
join {{ ref('fct_grocery_basket_daily') }} baseline
  on bounds.basket_id = baseline.basket_id
 and bounds.basket_item_key = baseline.basket_item_key
 and bounds.location_id = baseline.location_id
 and bounds.baseline_date = baseline.snapshot_date
join {{ ref('fct_grocery_basket_daily') }} latest_observation
  on bounds.basket_id = latest_observation.basket_id
 and bounds.basket_item_key = latest_observation.basket_item_key
 and bounds.location_id = latest_observation.location_id
 and bounds.comparison_date = latest_observation.snapshot_date
