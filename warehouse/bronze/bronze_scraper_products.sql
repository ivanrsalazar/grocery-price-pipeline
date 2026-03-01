CREATE TABLE IF NOT EXISTS `grocery-pipe-line.grocery_bronze.bronze_scraper_products` (
  snapshot_date  DATE,
  site           STRING,
  zipcode        STRING,
  category       STRING,
  search_term    STRING,
  product_name   STRING,
  price          STRING,
  product_url    STRING,
  store_name     STRING,
  scraped_at     TIMESTAMP,
  ingestion_ts   TIMESTAMP,
  raw            JSON
)
PARTITION BY snapshot_date
CLUSTER BY site, category;
