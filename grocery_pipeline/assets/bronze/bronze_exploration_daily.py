import json
import hashlib
import os
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import yaml
import requests
from google.cloud import bigquery
from dagster import asset, AssetExecutionContext, RetryPolicy

from grocery_pipeline.settings import get_bq_project

# =====================================================
# Constants
# =====================================================

KROGER_OAUTH_URL = "https://api.kroger.com/v1/connect/oauth2/token"
KROGER_PRODUCTS_URL = "https://api.kroger.com/v1/products"

BQ_PROJECT = get_bq_project()
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
MAX_REQUEST_ATTEMPTS = 4
TOKEN_REFRESH_SKEW_SECONDS = 60

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "canonical_search_terms.yaml"

# =====================================================
# Auth
# =====================================================


class KrogerTokenExpired(RuntimeError):
    """Raised when Kroger rejects an access token during a product request."""


def _fetch_kroger_access_token(session=requests) -> tuple[str, int]:
    resp = session.post(
        KROGER_OAUTH_URL,
        auth=(
            os.environ["KROGER_CLIENT_ID"],
            os.environ["KROGER_CLIENT_SECRET"],
        ),
        data={"grant_type": "client_credentials", "scope": "product.compact"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload["access_token"], int(payload.get("expires_in", 1800))


def get_kroger_access_token() -> str:
    """Fetch a token for one-off callers and credential checks."""
    token, _ = _fetch_kroger_access_token()
    return token


class KrogerTokenManager:
    """Cache and proactively refresh the short-lived Kroger OAuth token."""

    def __init__(self, session):
        self._session = session
        self._token = None
        self._refresh_at = 0.0

    def get(self) -> str:
        if self._token is None or time.monotonic() >= self._refresh_at:
            return self.refresh()
        return self._token

    def refresh(self) -> str:
        token, expires_in = _fetch_kroger_access_token(self._session)
        refresh_after = max(1, expires_in - TOKEN_REFRESH_SKEW_SECONDS)
        self._token = token
        self._refresh_at = time.monotonic() + refresh_after
        return token


# =====================================================
# API
# =====================================================


def search_products(
    token: str,
    term: str,
    location_id: str,
    limit: int = 25,
    session=requests,
) -> List[dict]:
    for attempt in range(MAX_REQUEST_ATTEMPTS):
        try:
            resp = session.get(
                KROGER_PRODUCTS_URL,
                headers={"Authorization": f"Bearer {token}"},
                params={
                    "filter.term": term,
                    "filter.locationId": location_id,
                    "filter.limit": limit,
                },
                timeout=30,
            )
        except (requests.Timeout, requests.ConnectionError):
            if attempt == MAX_REQUEST_ATTEMPTS - 1:
                raise
            time.sleep(min(2 ** attempt, 8) + random.random())
            continue

        if resp.status_code == 401:
            raise KrogerTokenExpired("Kroger access token expired")

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == MAX_REQUEST_ATTEMPTS - 1:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else 2 ** attempt
            except ValueError:
                delay = 2 ** attempt
            time.sleep(min(delay, 60) + random.random())
            continue

        resp.raise_for_status()
        return resp.json().get("data", [])

    raise RuntimeError("Kroger product request exhausted all retry attempts")


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


def _row_insert_id(row: Dict) -> str:
    """Build a stable insert ID so retries do not duplicate a daily product row."""
    key = "|".join(
        str(row.get(field) or "")
        for field in ("snapshot_date", "location_id", "search_term", "product_id")
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# =====================================================
# Asset
# =====================================================


@asset(
    name="exploration_kroger_products_daily",
    retry_policy=RetryPolicy(max_retries=5, delay=180),
    compute_kind="python",
)
def exploration_kroger_products_daily(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=BQ_PROJECT)
    session = requests.Session()
    tokens = KrogerTokenManager(session)

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
            try:
                products = search_products(
                    tokens.get(), term, location_id, session=session
                )
            except KrogerTokenExpired:
                context.log.info(
                    "Kroger access token expired; refreshing and retrying "
                    f"term={term!r} location_id={location_id}"
                )
                products = search_products(
                    tokens.refresh(), term, location_id, session=session
                )

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
                    errors = client.insert_rows_json(
                        TABLE_ID,
                        rows,
                        row_ids=[_row_insert_id(row) for row in rows],
                    )
                    if errors:
                        raise RuntimeError(f"BigQuery insert errors: {errors}")

                    total_inserted += len(rows)
                    context.log.info(f"Flushed {len(rows)} rows (total={total_inserted})")
                    rows.clear()

            time.sleep(REQUEST_SLEEP + random.random() * 0.1)

    # final flush
    if rows:
        errors = client.insert_rows_json(
            TABLE_ID,
            rows,
            row_ids=[_row_insert_id(row) for row in rows],
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        total_inserted += len(rows)
        context.log.info(f"Final flush {len(rows)} rows")

    context.log.info(f"Completed exploration ingestion | total rows={total_inserted}")
