import os
import json
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests.exceptions import HTTPError
from google.cloud import bigquery
from dagster import asset, AssetExecutionContext

# -----------------------
# Constants
# -----------------------

KROGER_OAUTH_URL = "https://api.kroger.com/v1/connect/oauth2/token"
KROGER_PRODUCTS_URL = "https://api.kroger.com/v1/products"

# Fixed location for pricing (Foodsco - Folsom)
LOCATION_ID = "70400357"

BQ_PROJECT = "grocery-pipe-line"
BQ_DATASET = "grocery_bronze"
BQ_TABLE = "kroger_product_snapshot"

LOCAL_BRONZE_DIR = Path("/home/ubuntu/data/bronze")

CANONICAL_PRODUCTS: Dict[str, str] = {
    "MILK_WHOLE": "whole milk",
    "MILK_2_PERCENT": "2% milk",
    "EGGS_LARGE": "large eggs",
    "BUTTER_SALTED": "salted butter",
    "CHEESE_AMERICAN": "american cheese",
    "YOGURT_PLAIN": "plain yogurt",
    "BREAD_WHITE": "white bread",
    "BREAD_WHOLE_WHEAT": "whole wheat bread",
    "BANANAS": "bananas",
    "APPLES_GALA": "gala apples",
    "ONIONS_YELLOW": "yellow onions",
    "POTATOES_RUSSET": "russet potatoes",
    "CHICKEN_BREAST_RAW": "chicken breast",
    "GROUND_BEEF_80_20": "ground beef 80/20",
    "RICE_WHITE_LONG": "long grain white rice",
    "PASTA_SPAGHETTI": "spaghetti pasta",
    "FLOUR_ALL_PURPOSE": "all purpose flour",
    "SUGAR_GRANULATED": "granulated sugar",
    "COFFEE_GROUND": "ground coffee",
    "TEA_BLACK": "black tea",
}

# -----------------------
# HTTP helpers
# -----------------------

def get_kroger_access_token() -> str:
    resp = requests.post(
        KROGER_OAUTH_URL,
        auth=(
            os.environ["KROGER_CLIENT_ID"],
            os.environ["KROGER_CLIENT_SECRET"],
        ),
        data={
            "grant_type": "client_credentials",
            "scope": "product.compact",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_with_retries(
    url: str,
    headers: dict,
    params: dict,
    retries: int = 3,
    base_backoff: float = 1.5,
) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            return resp

        except HTTPError as e:
            status = e.response.status_code if e.response else None
            if status and status >= 500 and attempt < retries:
                time.sleep((base_backoff ** attempt) + random.random())
                continue
            raise

    return None


def search_product(token: str, term: str) -> Optional[dict]:
    resp = get_with_retries(
        KROGER_PRODUCTS_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "filter.term": term,
            "filter.limit": 1,
        },
    )
    if not resp:
        return None

    data = resp.json().get("data", [])
    return data[0] if data else None


def get_product_details(token: str, product_id: str) -> Optional[dict]:
    resp = get_with_retries(
        f"{KROGER_PRODUCTS_URL}/{product_id}",
        headers={"Authorization": f"Bearer {token}"},
        params={"filter.locationId": LOCATION_ID},
    )
    return resp.json() if resp else None


def extract_price(details: dict) -> Optional[float]:
    """
    Kroger pricing is deeply nested and frequently missing.
    This handles all known cases safely.
    """
    try:
        items = details.get("data", {}).get("items", [])
        if not items:
            return None

        price = items[0].get("price", {})
        return price.get("regular") or price.get("promo")
    except Exception:
        return None


# -----------------------
# Asset
# -----------------------

@asset(
    name="bronze_kroger_products",
    compute_kind="kroger_api",
)
def bronze_kroger_products(context: AssetExecutionContext):
    snapshot_ts = datetime.now(timezone.utc)
    snapshot_ts_iso = snapshot_ts.isoformat()

    context.log.info("Authenticating with Kroger API")
    token = get_kroger_access_token()

    records: List[dict] = []

    for canonical_id, search_term in CANONICAL_PRODUCTS.items():
        product = search_product(token, search_term)

        if not product:
            context.log.warning(f"No search result for {canonical_id}")
            continue

        product_id = product.get("productId")
        details = get_product_details(token, product_id)

        price = extract_price(details) if details else None

        record = {
            "snapshot_ts": snapshot_ts_iso,
            "location_id": LOCATION_ID,
            "canonical_product_id": canonical_id,
            "search_term": search_term,
            "product_id": product_id,
            "brand": product.get("brand"),
            "description": product.get("description"),
            "price": price,
            "raw_search": product,
            "raw_details": details,
        }

        records.append(record)
        context.log.info(
            f"{canonical_id} → {product_id} | price={price}"
        )

        time.sleep(0.2 + random.random() * 0.3)

    if not records:
        raise RuntimeError("No products captured from Kroger API")

    # -----------------------
    # Local snapshot
    # -----------------------

    LOCAL_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_BRONZE_DIR / f"kroger_product_snapshot_{snapshot_ts.date()}.jsonl"

    with local_path.open("a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    # -----------------------
    # Upload to BigQuery
    # -----------------------

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(
        records,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    context.log.info(f"Loaded {len(records)} rows into {table_id}")

    return {
        "snapshot_ts": snapshot_ts_iso,
        "records_written": len(records),
        "location_id": LOCATION_ID,
        "bq_table": table_id,
        "local_path": str(local_path),
    }