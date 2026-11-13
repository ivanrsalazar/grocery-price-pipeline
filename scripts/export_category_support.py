#!/usr/bin/env python3
"""Export the category-index audit mart for the static portfolio dashboard."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "grocery-pipe-line"
QUERY = """
select
    city,
    grocery_category,
    search_term,
    normalized_unit,
    sku,
    sku_unit_price_percent_change,
    baseline_description,
    baseline_size,
    baseline_shelf_price,
    baseline_unit_price,
    comparison_description,
    comparison_size,
    comparison_shelf_price,
    comparison_unit_price
from `grocery-pipe-line.grocery_gold.mart_category_price_support`
order by market_order, grocery_category, search_term, normalized_unit
"""


def optional_float(value: object) -> float | None:
    return None if value is None else round(float(value), 6)


def serialize(row: bigquery.table.Row) -> dict[str, object]:
    return {
        "city": row.city,
        "category": row.grocery_category,
        "term": row.search_term,
        "unit": row.normalized_unit,
        "sku": row.sku,
        "change": optional_float(row.sku_unit_price_percent_change),
        "april": {
            "description": row.baseline_description,
            "size": row.baseline_size,
            "shelfPrice": optional_float(row.baseline_shelf_price),
            "unitPrice": optional_float(row.baseline_unit_price),
        },
        "november": {
            "description": row.comparison_description,
            "size": row.comparison_size,
            "shelfPrice": optional_float(row.comparison_shelf_price),
            "unitPrice": optional_float(row.comparison_unit_price),
        },
    }


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    default_output = (
        repo_root
        / "sites"
        / "four-city-grocery-basket"
        / "dist"
        / "category-support.json"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=default_output)
    args = parser.parse_args()

    rows = bigquery.Client(project=PROJECT_ID).query(QUERY).result()
    payload = [serialize(row) for row in rows]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Wrote {len(payload)} category-support rows to {args.output}")


if __name__ == "__main__":
    main()
