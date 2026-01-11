CREATE TABLE IF NOT EXISTS `grocery-pipe-line.grocery_bronze.bronze_kroger_locations` (
    snapshot_ts       TIMESTAMP NOT NULL,
    location_id       STRING NOT NULL,
    raw_payload       JSON NOT NULL,
    source            STRING NOT NULL,
    ingestion_run_id  STRING
)
PARTITION BY DATE(snapshot_ts)
CLUSTER BY location_id;