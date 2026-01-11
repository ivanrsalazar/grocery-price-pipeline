DROP TABLE IF EXISTS `grocery-pipe-line.grocery_bronze.kroger_product_snapshot`;

CREATE TABLE `grocery-pipe-line.grocery_bronze.kroger_product_snapshot` (
  snapshot_ts TIMESTAMP NOT NULL,
  canonical_product_id STRING NOT NULL,
  search_term STRING,
  product_id STRING,
  location_id STRING,
  price FLOAT64,
  brand STRING,
  description STRING,
  categories ARRAY<STRING>,

  -- raw payloads
  raw JSON,
  raw_search JSON,
  raw_details JSON
);