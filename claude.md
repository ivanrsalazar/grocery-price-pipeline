# Grocery Pipeline

Daily grocery product price monitoring pipeline. Ingests product data from the Kroger API across multiple US store locations, filters and normalizes it through a config-driven matching system, and loads it into BigQuery.

## Architecture

```
Kroger REST API (OAuth2)
    |
    v
[Bronze] exploration_kroger_products_daily    -- Python asset, inserts raw API responses
    |
    v
[Silver] silver_exploration_kroger_products   -- BigQuery MERGE with multi-strategy matching
    |
    v
[Gold]   (planned)                            -- Analytics-ready aggregates
```

**Warehouse**: Google BigQuery
**Project**: `grocery-pipe-line`
**Datasets**: `grocery_bronze`, `grocery_silver`, `grocery_gold`
**Schedule**: Daily at 06:00 UTC

## Directory Structure

```
grocery_pipeline/
  grocery_pipeline/
    definitions.py          Dagster Definitions entry point
    assets/
      bronze/
        bronze_exploration_daily.py    Main Kroger ingestion asset
        bronze_kroger_products_daily.py  Alternative canonical product asset
      silver/
        silver_exploration_kroger_products.py  Silver MERGE with matching logic
        missing_terms.py               QA: finds terms missing from bronze/silver
    config/
      canonical_search_terms.yaml      Search terms organized by category bucket
      canonical_categories.yaml        Expected categories + metadata per bucket
      canonical_search_term_rules.yaml Matching strategies and false-positive filters
    jobs/
      jobs.py                          Job definition
    schedules/
      schedules.py                     Daily cron schedule
    resources/
      bigquery.py                      BigQuery client resource (ambient GCP creds)
    scripts/
      eval_silver_false_positives.py   LLM-based data quality evaluation (OpenAI)
  warehouse/
    migrations/001_init.sql            Schema initialization DDL
    bronze/                            Bronze table DDL
    silver/                            Silver table DDL
```

## Asset DAG

The job `bronze_kroger_products_daily_job` materializes two assets in sequence:

1. **`exploration_kroger_products_daily`** (Bronze)
   - Authenticates via Kroger OAuth2 (`KROGER_CLIENT_ID` / `KROGER_CLIENT_SECRET`)
   - Loads search terms from `config/canonical_search_terms.yaml`
   - Iterates: 5 locations x ~250 search terms
   - Batch-inserts rows into `grocery_bronze.exploration_kroger_products` (500-row batches)
   - Rate limited: `0.15s + random(0.1)` sleep between API calls
   - Retry policy: 5 retries, 180s delay

2. **`silver_exploration_kroger_products`** (Silver)
   - Runs a BigQuery MERGE from bronze -> silver
   - Applies config-driven matching logic (see below)
   - Deduplicates via `ROW_NUMBER() OVER (PARTITION BY snapshot_date, location_id, search_term, product_id ORDER BY ingestion_ts DESC)`
   - Only rows passing all match criteria (`kept = true`) are written

## Config-Driven Matching System

This is the most complex part of the pipeline. Three YAML files control which products pass from bronze to silver.

### `canonical_search_terms.yaml`

Maps ~25 category buckets to lists of search terms. Each bucket has `breadth` (broad terms) and `drilldown` (specific terms):

```yaml
search_terms:
  produce:
    breadth: [apples, bananas, ...]
    drilldown: [gala apples, organic bananas, ...]
```

### `canonical_categories.yaml`

Maps each bucket key to its expected Kroger category string, temperature default, and SNAP eligibility:

```yaml
categories:
  produce:
    expected_category: "Produce"
    temperature: REFRIGERATED
    snap_eligible: true
```

### `canonical_search_term_rules.yaml`

Defines per-term matching strategies and false-positive filters. Strategies:

| Strategy | Behavior |
|---|---|
| `all_tokens` (default) | ALL tokens from the search term must appear in description_norm |
| `any_of` | ANY token from the `any_of` list must appear |
| `require_any_of` | At least ONE token from `require_any_of` must appear |
| `require_all_of` | ALL tokens from `require_all_of` must appear |
| `none_of` | NONE of the `none_of` tokens may appear (also applied as a secondary filter on other strategies) |

Example rule:
```yaml
rules:
  frozen broccoli:
    strategy: all_tokens
    none_of: [meal, alfredo, cheese, stuffed, medley]
```

### How Matching Works in Silver SQL

A product is kept when ALL THREE conditions are true:
1. **`matched_category`**: The normalized expected category appears as a substring in the product's normalized categories string
2. **`matched_all_tokens`**: The strategy-specific token match passes
3. **`passed_none_of`**: No `none_of` tokens appear in the description (applied alongside any strategy)

When editing matching rules, modify the YAML files — the silver asset dynamically builds the SQL from these configs at runtime via `build_term_records()`.

## BigQuery Tables

**Bronze** — `grocery_bronze.exploration_kroger_products`:
- Key columns: `snapshot_date`, `search_term`, `location_id`, `product_id`, `brand`, `description`, `categories`, `price`, `raw` (JSON)
- Partitioned by: `DATE(ingestion_ts)`

**Silver** — `grocery_silver.exploration_kroger_products`:
- Inherits all bronze columns plus QA columns: `term_tokens`, `description_norm`, `categories_norm`, `matched_all_tokens`, `matched_category`, `kept`, `canonical_category`, `bucket_key`
- Partitioned by: `snapshot_date`
- Clustered by: `location_id`, `search_term`, `product_id`
- Only contains rows where `kept = true`

## Store Locations

Hardcoded in `bronze_exploration_daily.py`:

| Location ID | Zip | City |
|---|---|---|
| 70300022 | 90012 | LA |
| 53100541 | 60007 | Chicago |
| 542FC805 | 10001 | NYC |
| 03500529 | 75204 | Dallas |
| 70500808 | 98039 | Seattle |

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `KROGER_CLIENT_ID` | Yes | Kroger API OAuth2 client ID |
| `KROGER_CLIENT_SECRET` | Yes | Kroger API OAuth2 client secret |
| GCP ambient credentials | Yes | BigQuery access via VM service account |
| `OPENAI_API_KEY` | Scripts only | Used by `eval_silver_false_positives.py` |

## Coding Patterns

- **SQL in Python**: The silver asset builds a large BigQuery MERGE query as an f-string, injecting config from YAML files as `UNNEST([STRUCT(...)])` arrays. This is the core pattern — new matching rules go in YAML, not in SQL.
- **Batch inserts**: Bronze uses `client.insert_rows_json()` with a 500-row batch size to manage memory.
- **No partitioned jobs**: Unlike kline_pipeline, this pipeline uses simple daily scheduling without Dagster partition definitions.
- **Config paths**: Resolved relative to the source file using `Path(__file__).resolve().parents[2] / "config"`.
- **Normalization**: Text normalization for matching uses `REGEXP_REPLACE(LOWER(...), r"[^a-z0-9]+", " ")` consistently in both Python and BigQuery SQL.

## Future Scope

Additional grocery sources (Walmart, Albertson's, Whole Foods) will be added as new data sources within this pipeline, not as separate pipelines.
