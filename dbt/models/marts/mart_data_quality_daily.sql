{{
  config(
    partition_by={"field": "snapshot_date", "data_type": "date"}
  )
}}

select
    snapshot_date,
    count(*) as source_row_count,
    count(distinct to_json_string(struct(location_id, search_term, product_id))) as unique_key_count,
    count(*) - count(distinct to_json_string(struct(location_id, search_term, product_id))) as duplicate_row_count,
    count(distinct location_id) as location_count,
    count(distinct search_term) as search_term_count,
    countif(price is null or price <= 0) as invalid_price_count,
    max(ingestion_ts) as latest_ingestion_ts
from {{ source('grocery_silver', 'exploration_kroger_products') }}
group by snapshot_date
