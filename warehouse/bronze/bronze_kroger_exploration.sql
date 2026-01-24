CREATE TABLE IF NOT EXISTS `grocery-pipe-line.grocery_bronze.exploration_kroger_products` (
  snapshot_date DATE,
  search_term STRING,
  location_id STRING,

  product_id STRING,
  upc STRING,

  brand STRING,
  categories STRING,
  country_of_origin STRING,
  description STRING,
  receipt_description STRING,

  snap_eligible BOOL,

  price FLOAT64,
  size STRING,
  sold_by STRING,

  gross_weight STRING,
  net_weight STRING,

  temperature STRING,

  ingredient_statement STRING,
  raw                    JSON,

  ingestion_ts TIMESTAMP
);