import os
import time
import json
import random
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests
from requests.exceptions import HTTPError
from google.cloud import bigquery
from dagster import asset, AssetExecutionContext

# =====================================================
# Constants
# =====================================================

KROGER_OAUTH_URL = "https://api.kroger.com/v1/connect/oauth2/token"
KROGER_PRODUCTS_URL = "https://api.kroger.com/v1/products"

BQ_PROJECT = "grocery-pipe-line"
BQ_DATASET = "grocery_bronze"
BQ_TABLE = "bronze_kroger_products"

DATE_FMT = "%Y-%m-%d"

LOCATION_IDS = [
    "70300022",
    "70400770",
    "70400363",
    "70400375",
    "70300032",
    "70300294",
    "70400330",
    "70300016",
    "70400766",
    "70400381",
]

# =====================================================
# Canonical product specs (30)
# =====================================================

CANONICAL_PRODUCTS: List[Dict] = [
    # Produce
    {"term": "bananas", "category": "Produce", "temp": "AMBIENT", "snap": True},
    {"term": "gala apples", "category": "Produce", "temp": "AMBIENT", "snap": True},
    {"term": "strawberries", "category": "Produce", "temp": "REFRIGERATED", "snap": True},
    {"term": "blueberries", "category": "Produce", "temp": "REFRIGERATED", "snap": True},
    {"term": "russet potatoes", "category": "Produce", "temp": "AMBIENT", "snap": True},
    {"term": "yellow onions", "category": "Produce", "temp": "AMBIENT", "snap": True},
    {"term": "romaine lettuce", "category": "Produce", "temp": "REFRIGERATED", "snap": True},
    {"term": "carrots", "category": "Produce", "temp": "REFRIGERATED", "snap": True},
    {"term": "avocados", "category": "Produce", "temp": "AMBIENT", "snap": True},
    {"term": "fresh spinach", "category": "Produce", "temp": "REFRIGERATED", "snap": True},

    # Dairy
    {"term": "whole milk", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "2% milk", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "skim milk", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "large eggs", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "salted butter", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "unsalted butter", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "cheddar cheese", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "mozzarella cheese", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "plain yogurt", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},
    {"term": "greek yogurt", "category": "Dairy", "temp": "REFRIGERATED", "snap": True},

    # Meat & Seafood
    {"term": "chicken breast", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "chicken thighs", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "ground beef", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "beef steak", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "pork chops", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "bacon", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "salmon fillet", "category": "Meat & Seafood", "temp": "REFRIGERATED", "snap": True},
    {"term": "frozen shrimp", "category": "Meat & Seafood", "temp": "FROZEN", "snap": True},
    {"term": "frozen tilapia", "category": "Meat & Seafood", "temp": "FROZEN", "snap": True},
    {"term": "frozen chicken nuggets", "category": "Meat & Seafood", "temp": "FROZEN", "snap": True},
]

# =====================================================
# Normalization helpers
# =====================================================

def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def normalize_categories(categories: Optional[List[str]]) -> List[str]:
    return [norm(c) for c in categories or []]


def normalize_temperature(temp: Optional[str]) -> Optional[str]:
    if not temp:
        return None
    t = norm(temp)
    if "frozen" in t:
        return "FROZEN"
    if "refriger" in t:
        return "REFRIGERATED"
    if "ambient" in t:
        return "AMBIENT"
    return None


# =====================================================
# Auth + API
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


def search_products(token: str, term: str, location_id: str) -> List[dict]:
    resp = requests.get(
        KROGER_PRODUCTS_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "filter.term": term,
            "filter.locationId": location_id,
            "filter.limit": 10,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


# =====================================================
# Asset
# =====================================================

@asset(
    name="bronze_kroger_products_daily",
    compute_kind="kroger_api",
)
def bronze_kroger_products_daily(context: AssetExecutionContext):
    run_date = datetime.now(timezone.utc).strftime(DATE_FMT)
    token = get_kroger_access_token()
    bq_client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    rows: List[Dict] = []
    rejected = 0

    for location_id in LOCATION_IDS:
        for spec in CANONICAL_PRODUCTS:
            term = spec["term"]
            expected_cat = norm(spec["category"])
            expected_temp = spec["temp"]
            expected_snap = spec["snap"]

            products = search_products(token, term, location_id)
            context.log.info(f"{term} @ {location_id} → {len(products)} candidates")

            for p in products:
                categories = normalize_categories(p.get("categories"))
                temperature = normalize_temperature(p.get("temperature", {}).get("indicator"))
                snap = p.get("snapEligible")

                if expected_cat not in categories:
                    rejected += 1
                    continue

                if temperature != expected_temp:
                    rejected += 1
                    continue

                if snap != expected_snap:
                    rejected += 1
                    continue

                item = (p.get("items") or [{}])[0]
                nutrition = (p.get("nutritionInformation") or [{}])[0]

                rows.append(
                    {
                        "snapshot_date": run_date,
                        "search_term": term,
                        "location_id": location_id,
                        "brand": p.get("brand"),
                        "categories": p.get("categories"),
                        "country_of_origin": p.get("countryOrigin"),
                        "description": p.get("description"),
                        "snap_eligible": snap,
                        "receipt_description": p.get("receiptDescription"),
                        "price": item.get("price", {}).get("regular"),
                        "size": item.get("size"),
                        "sold_by": item.get("soldBy"),
                        "gross_weight": p.get("itemInformation", {}).get("grossWeight"),
                        "net_weight": p.get("itemInformation", {}).get("netWeight"),
                        "temperature": temperature,
                        "ingredient_statement": nutrition.get("ingredientStatement"),
                        "raw_product_id": p.get("productId"),
                    }
                )

            time.sleep(0.25 + random.random() * 0.25)

    if not rows:
        raise RuntimeError("No matching Kroger products after filtering")

    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    context.log.info(
        f"Inserted {len(rows)} rows | Rejected {rejected} non-matching candidates"
    )

    return {
        "snapshot_date": run_date,
        "rows_inserted": len(rows),
        "rows_rejected": rejected,
        "locations": len(LOCATION_IDS),
        "search_terms": len(CANONICAL_PRODUCTS),
    }