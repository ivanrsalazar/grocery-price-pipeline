{{
  config(
    materialized="table",
    partition_by={"field": "snapshot_date", "data_type": "date"},
    cluster_by=["location_id", "search_term", "product_id"]
  )
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date, location_id, search_term, product_id
            order by ingestion_ts desc
        ) as row_number_for_key
    from {{ source('grocery_silver', 'exploration_kroger_products') }}
),

deduplicated as (
    select * except (row_number_for_key)
    from ranked
    where row_number_for_key = 1
)

select
    to_hex(md5(concat(
        cast(snapshot_date as string), '|',
        location_id, '|', search_term, '|', product_id
    ))) as observation_key,
    snapshot_date,
    ingestion_ts,
    location_id,
    search_term,
    product_id,
    upc,
    brand,
    description,
    receipt_description,
    categories,
    canonical_category,
    bucket_key,
    cast(price as numeric) as price,
    size,
    sold_by,
    snap_eligible,
    country_of_origin,
    gross_weight,
    net_weight,
    temperature,
    ingredient_statement,
    raw
from deduplicated
