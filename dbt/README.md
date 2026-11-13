# Grocery Analytics dbt Project

This project turns the deduplicated Kroger silver feed into documented,
tested gold-layer models in `grocery-pipe-line.grocery_gold`.

## Basket methodologies

- `canonical_essentials`: twelve grocery concepts normalized to a fixed weight,
  volume, or count. The standardized mart reports both the cheapest eligible
  product and the market median, removing package-size differences while still
  showing the range between bargain shopping and a typical option.
- `matched_sku`: eight identical UPCs available in every market on the April 7
  and November 12 comparison dates. This improves product comparability but is
  biased toward nationally distributed products.

Neither result should be described as an official inflation measure. The
canonical basket is an assortment index; the matched-SKU basket is a fixed
product index.

### Standard quantities

| Item | Quantity |
|---|---:|
| Apples, tomatoes, ground beef, chicken thighs | 16 oz |
| Butter | 16 oz |
| White bread | 20 oz |
| Black beans | 15 oz |
| Frozen broccoli, cereal | 12 oz |
| Whole milk | 64 fl oz |
| Orange juice | 52 fl oz |
| Eggs | 12 count |

Ambiguous multipack size strings are excluded from standardized price
calculations. `mart_standardized_basket_daily` contains complete basket totals;
`fct_standardized_basket_daily` contains the item-level contributions.
`mart_cheapest_basket_products` retains the actual fixed product, package size,
shelf price, and standardized contribution for reproducible comparisons. For
each city and category, the model selects the cheapest April product that is
also present in November with the same product ID and package quantity, then
tracks that exact product over time. It does not substitute a different size,
grade, or SKU merely because that alternative is cheaper in November.

`mart_fixed_basket_item_movers` ranks the largest positive and negative item
contributions within each city and adds cross-market median changes, direction
counts, and ranges.

The dashboard's broader category view comes from
`mart_category_price_movers`. It is intentionally independent of the 12-item
basket: the model retains only exact product IDs observed in the same city,
search term, and normalized unit on both comparison dates. It calculates a
geometric price change across matched SKUs within each term, then gives every
term equal weight in its category. This produces 6–31 comparable terms and
15–336 SKU matches per category and city without allowing larger categories to
dominate purely because they contain more configured searches.

`mart_fixed_basket_category_movers` remains available for decomposing the
12-item fixed basket into dollar contributions; it is not used for the broad
category chart.

`mart_category_price_support` makes the category calculation auditable. It
stores every exact product-ID pair used by the index, including both dates'
descriptions, package sizes, shelf prices, and normalized unit prices.
Refresh the dashboard's static audit data after a dbt build with:

```bash
.venv/bin/python scripts/export_category_support.py
```

## Commands

```bash
cd /Users/ivansalazar/repos/grocery-price-pipeline

.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt
.venv/bin/dbt source freshness --project-dir dbt --profiles-dir dbt
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt
.venv/bin/dbt docs generate --project-dir dbt --profiles-dir dbt
.venv/bin/dbt docs serve --project-dir dbt --profiles-dir dbt --port 8080
```

Authentication uses local Google OAuth/Application Default Credentials. No
service-account key is stored in the repository.
