import re
import sys
from pathlib import Path
from typing import Set, List

import yaml
from google.cloud import bigquery

# =====================================================
# BigQuery tables / location
# =====================================================

BQ_PROJECT = "grocery-pipe-line"
BQ_LOCATION = "us-east1"

BRONZE_TABLE = f"{BQ_PROJECT}.grocery_bronze.exploration_kroger_products"
SILVER_TABLE = f"{BQ_PROJECT}.grocery_silver.exploration_kroger_products"

# =====================================================
# Config path
# =====================================================

# Assumes this script lives at: grocery_pipeline/assets/silver/<this_file>.py
# so parents[2] -> grocery_pipeline/
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "canonical_search_terms.yaml"


# =====================================================
# Helpers
# =====================================================

def norm_term(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def load_canonical_terms() -> Set[str]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"canonical_search_terms.yaml not found at: {CONFIG_PATH}")

    with CONFIG_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    st = cfg.get("search_terms") or {}
    terms: Set[str] = set()

    for block in st.values():
        block = block or {}
        for t in (block.get("breadth") or []) + (block.get("drilldown") or []):
            nt = norm_term(t)
            if nt:
                terms.add(nt)

    if not terms:
        raise RuntimeError("No canonical search terms loaded from YAML (check file format).")

    return terms


def fetch_distinct_terms(client: bigquery.Client, table_id: str) -> Set[str]:
    query = f"""
      SELECT DISTINCT
        REGEXP_REPLACE(LOWER(COALESCE(search_term, "")), r'\\s+', ' ') AS term
      FROM `{table_id}`
      WHERE search_term IS NOT NULL AND TRIM(search_term) != ""
    """
    return {row["term"] for row in client.query(query, location=BQ_LOCATION).result() if row["term"]}


def print_list(title: str, items: List[str], limit: int = 500) -> None:
    print("\n" + "=" * 80)
    print(f"{title} (count={len(items)})")
    print("=" * 80)
    if not items:
        print("(none)")
        return
    for i, x in enumerate(items[:limit], 1):
        print(f"{i:>4}. {x}")
    if len(items) > limit:
        print(f"... truncated, showing first {limit} ...")


# =====================================================
# Main
# =====================================================

def main() -> int:
    canonical = load_canonical_terms()
    print(f"Loaded canonical terms: {len(canonical)}")
    print(f"Config: {CONFIG_PATH}")

    client = bigquery.Client(project=BQ_PROJECT)

    bronze_terms = fetch_distinct_terms(client, BRONZE_TABLE)
    silver_terms = fetch_distinct_terms(client, SILVER_TABLE)

    print(f"Distinct terms in BRONZE: {len(bronze_terms)}")
    print(f"Distinct terms in SILVER: {len(silver_terms)}")

    canonical_in_bronze = canonical & bronze_terms
    missing_from_silver = sorted(canonical_in_bronze - silver_terms)

    canonical_not_in_bronze = sorted(canonical - bronze_terms)  # optional but helpful

    print_list(
        "Canonical terms that appear in BRONZE but are MISSING in SILVER",
        missing_from_silver,
    )

    print_list(
        "Canonical terms that NEVER appeared in BRONZE (so they can't make it to SILVER)",
        canonical_not_in_bronze,
    )

    # quick summary
    print("\n" + "-" * 80)
    print("SUMMARY")
    print("-" * 80)
    print(f"canonical total:                 {len(canonical)}")
    print(f"canonical present in bronze:     {len(canonical_in_bronze)}")
    print(f"canonical missing from silver:   {len(missing_from_silver)}")
    print(f"canonical never in bronze:       {len(canonical_not_in_bronze)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())