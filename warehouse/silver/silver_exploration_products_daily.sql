CREATE SCHEMA IF NOT EXISTS `grocery-pipe-line.grocery_silver`;

CREATE TABLE IF NOT EXISTS `grocery-pipe-line.grocery_silver.exploration_kroger_products` (
  -- lineage
  snapshot_date        DATE,
  search_term          STRING,
  location_id          STRING,
  ingestion_ts         TIMESTAMP,

  -- identifiers
  product_id           STRING,
  upc                  STRING,

  -- product attributes (normalized / curated)
  brand                STRING,
  categories           STRING,
  country_of_origin    STRING,
  description          STRING,
  receipt_description  STRING,
  snap_eligible        BOOL,
  price                FLOAT64,
  size                 STRING,
  sold_by              STRING,
  gross_weight         STRING,
  net_weight           STRING,
  temperature          STRING,
  ingredient_statement STRING,

  -- filtering / QA metadata
  term_tokens          ARRAY<STRING>,
  description_norm     STRING,
  categories_norm      ARRAY<STRING>,
  matched_all_tokens   BOOL,
  matched_category     BOOL,
  kept                BOOL,

  -- keep the original payload for traceability
  raw                  JSON,

  canonical_category STRING,
  bucket_key STRING

)
PARTITION BY snapshot_date
CLUSTER BY location_id, search_term, product_id;