"""
Dagster configuration and BigQuery helpers for the retail scraper bronze assets.
"""

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import List

from dagster import Config
from google.cloud import bigquery

# =====================================================
# BigQuery Constants
# =====================================================

BQ_PROJECT = "grocery-pipe-line"
BQ_DATASET = "grocery_bronze"
BQ_TABLE = "bronze_scraper_products"
TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

BATCH_SIZE = 500


# =====================================================
# Dagster Config
# =====================================================


class ScraperConfig(Config):
    zipcode: str
    cdp_url: str = ""
    terms_file: str = ""
    max_per_query: int = 20
    delay_min: float = 0.1
    delay_max: float = 0.3
    page_timeout: int = 16000
    settle: float = 0.8
    scrape_wait: float = 0.3
    manual_gate: bool = True
    manual_token: str = "CLEAR"
    headful: bool = False


# =====================================================
# Transform helpers
# =====================================================


def products_to_bq_rows(products, snapshot_date: str, ingestion_ts: str) -> List[dict]:
    """Convert a list of Product dataclass instances to BigQuery row dicts."""
    rows = []
    for p in products:
        d = asdict(p)
        rows.append({
            "snapshot_date": snapshot_date,
            "site": d.get("site", ""),
            "zipcode": d.get("zipcode", ""),
            "category": d.get("category", ""),
            "search_term": d.get("query", ""),
            "product_name": d.get("name", ""),
            "price": d.get("price"),
            "product_url": d.get("url", ""),
            "store_name": d.get("store_name"),
            "scraped_at": d.get("scraped_at", ""),
            "ingestion_ts": ingestion_ts,
            "raw": json.dumps(d),
        })
    return rows


def flush_rows_to_bq(client: bigquery.Client, rows: List[dict], context) -> int:
    """Batch-insert rows to BigQuery. Returns number of rows inserted."""
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        errors = client.insert_rows_json(TABLE_ID, batch)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        total += len(batch)
        context.log.info(f"Flushed {len(batch)} rows (total={total})")
    return total
