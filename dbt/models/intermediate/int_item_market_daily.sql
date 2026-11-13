select
    snapshot_date,
    location_id,
    search_term,
    any_value(bucket_key) as bucket_key,
    any_value(canonical_category) as canonical_category,
    count(distinct product_id) as product_count,
    count(distinct brand) as brand_count,
    min(price) as minimum_price,
    approx_quantiles(price, 2)[offset(1)] as median_price,
    avg(price) as average_price,
    max(price) as maximum_price,
    stddev_pop(price) as price_stddev
from {{ ref('stg_kroger_products') }}
where price > 0
group by snapshot_date, location_id, search_term
