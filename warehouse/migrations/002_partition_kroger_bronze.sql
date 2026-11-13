-- One-time, non-destructive migration for the existing unpartitioned table.
-- The original table is retained as exploration_kroger_products_unpartitioned_backup.

CREATE TABLE IF NOT EXISTS
  `grocery-pipe-line.grocery_bronze.exploration_kroger_products_partitioned`
PARTITION BY snapshot_date
CLUSTER BY location_id, search_term, product_id
AS
SELECT *
FROM `grocery-pipe-line.grocery_bronze.exploration_kroger_products`;

ASSERT (
  SELECT COUNT(*)
  FROM `grocery-pipe-line.grocery_bronze.exploration_kroger_products_partitioned`
) = (
  SELECT COUNT(*)
  FROM `grocery-pipe-line.grocery_bronze.exploration_kroger_products`
) AS "Partitioned bronze copy does not match the original row count";

ALTER TABLE `grocery-pipe-line.grocery_bronze.exploration_kroger_products`
RENAME TO exploration_kroger_products_unpartitioned_backup;

ALTER TABLE `grocery-pipe-line.grocery_bronze.exploration_kroger_products_partitioned`
RENAME TO exploration_kroger_products;
