CREATE TABLE IF NOT EXISTS `grocery-price-pipeline.grocery_bronze.bronze_kroger_products` (
    snapshot_ts       TIMESTAMP NOT NULL,
    location_id       STRING NOT NULL,
    search_term       STRING,
    raw_payload       JSON NOT NULL,

    -- lineage / audit
    source            STRING NOT NULL,
    ingestion_run_id  STRING
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY location_id;