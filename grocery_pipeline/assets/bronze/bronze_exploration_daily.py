import json
import os
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import yaml
import requests
from google.cloud import bigquery
from dagster import asset, AssetExecutionContext

# =====================================================
# Constants
# =====================================================

KROGER_OAUTH_URL = "https://api.kroger.com/v1/connect/oauth2/token"
KROGER_PRODUCTS_URL = "https://api.kroger.com/v1/products"

BQ_PROJECT = "grocery-pipe-line"
BQ_DATASET = "grocery_bronze"
BQ_TABLE = "exploration_kroger_products"

TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

LOCATION_IDS = [            # Zip   | City
    "70300022",             # 90012 | LA
    "53100541",             # 60007 | Chicago    
    "542FC805",             # 10001 | NYC
    "03500529",             # 75204 | Dallas
    "70500808",             # 98039 | Seattle
]

BATCH_SIZE = 500
REQUEST_SLEEP = 0.15

CONFIG_PATH = (
    Path("~/pipelines/grocery_pipeline/grocery_pipeline/config/canonical_search_terms.yaml")
    .expanduser()
)

# =====================================================
# Auth
# =====================================================


def get_kroger_access_token() -> str:
    resp = requests.post(
        KROGER_OAUTH_URL,
        auth=(
            os.environ["KROGER_CLIENT_ID"],
            os.environ["KROGER_CLIENT_SECRET"],
        ),
        data={"grant_type": "client_credentials", "scope": "product.compact"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# =====================================================
# API
# =====================================================


def search_products(
    token: str,
    term: str,
    location_id: str,
    limit: int = 25,
) -> List[dict]:
    resp = requests.get(
        KROGER_PRODUCTS_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "filter.term": term,
            "filter.locationId": location_id,
            "filter.limit": limit,
        },
        timeout=30,
    )

    if resp.status_code >= 500:
        return []

    resp.raise_for_status()
    return resp.json().get("data", [])


# =====================================================
# YAML loader
# =====================================================


def load_search_terms() -> List[str]:
    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)

    terms: List[str] = []

    for category in cfg["search_terms"].values():
        terms.extend(category.get("breadth", []))
        terms.extend(category.get("drilldown", []))

    # de-duplicate, preserve order
    return list(dict.fromkeys(terms))


# =====================================================
# Asset
# =====================================================


@asset(
    name="exploration_kroger_products_daily",
    compute_kind="python",
)
def exploration_kroger_products_daily(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=BQ_PROJECT)
    token = get_kroger_access_token()

    search_terms = load_search_terms()

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    rows: List[Dict] = []
    total_inserted = 0

    context.log.info(
        f"Starting exploration ingestion | "
        f"{len(search_terms)} terms × {len(LOCATION_IDS)} locations"
    )

    for location_id in LOCATION_IDS:
        for term in search_terms:
            products = search_products(token, term, location_id)

            for p in products:
                item = (p.get("items") or [{}])[0]
                price = item.get("price", {})
                nutrition = (p.get("nutritionInformation") or [{}])[0]

                temperature = (
                    p.get("temperature", {}) or {}
                ).get("indicator")

                row = {
                    "snapshot_date": snapshot_date,
                    "search_term": term,
                    "location_id": location_id,

                    "product_id": p.get("productId"),
                    "upc": p.get("upc"),

                    "brand": p.get("brand"),
                    "categories": ",".join(p.get("categories") or []),
                    "country_of_origin": p.get("countryOrigin"),
                    "description": p.get("description"),
                    "receipt_description": p.get("receiptDescription"),

                    "snap_eligible": p.get("snapEligible"),

                    "price": price.get("regular"),
                    "size": item.get("size"),
                    "sold_by": item.get("soldBy"),

                    "gross_weight": p.get("itemInformation", {}).get("grossWeight"),
                    "net_weight": p.get("itemInformation", {}).get("netWeight"),

                    "temperature": temperature,
                    "ingredient_statement": nutrition.get("ingredientStatement"),

                    # IMPORTANT: JSON column must receive JSON string
                    "raw": json.dumps(p),

                    "ingestion_ts": ingestion_ts,
                }

                rows.append(row)

                # -------------------------------
                # Batch flush
                # -------------------------------
                if len(rows) >= BATCH_SIZE:
                    errors = client.insert_rows_json(TABLE_ID, rows)
                    if errors:
                        raise RuntimeError(f"BigQuery insert errors: {errors}")

                    total_inserted += len(rows)
                    context.log.info(f"Flushed {len(rows)} rows (total={total_inserted})")
                    rows.clear()

            time.sleep(REQUEST_SLEEP + random.random() * 0.1)

    # final flush
    if rows:
        errors = client.insert_rows_json(TABLE_ID, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        total_inserted += len(rows)
        context.log.info(f"Final flush {len(rows)} rows")

    context.log.info(f"Completed exploration ingestion | total rows={total_inserted}")