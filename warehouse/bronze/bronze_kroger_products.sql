DROP TABLE IF EXISTS `grocery-pipe-line.grocery_bronze.kroger_products`;

CREATE TABLE `grocery-pipe-line.grocery_bronze.kroger_products` (
  ingestion_ts     TIMESTAMP NOT NULL,
  source           STRING    NOT NULL,
  search_term      STRING,

  product_id       STRING    NOT NULL,
  upc              STRING,
  brand            STRING,
  description      STRING,
  categories       ARRAY<STRING>,

  size             STRING,
  temperature      STRING,
  organic_claim    STRING,
  non_gmo          BOOL,
  snap_eligible    BOOL,

  raw              JSON
)
PARTITION BY DATE(ingestion_ts)
CLUSTER BY product_id;