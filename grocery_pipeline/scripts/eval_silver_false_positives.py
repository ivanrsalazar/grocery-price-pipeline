#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import yaml
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from openai import OpenAI


# ==========================
# Defaults (override via CLI)
# ==========================
DEFAULT_BQ_PROJECT = "grocery-pipe-line"
DEFAULT_SOURCE_TABLE = (
    "grocery-pipe-line.grocery_silver.exploration_kroger_products__backup_20260131"
)

# canonical_search_terms.yaml lives here (per your repo layout)
DEFAULT_CANONICAL_TERMS_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "canonical_search_terms.yaml"
)

# fallback if you don't pass --terms / --all-terms (kept from earlier)
DEFAULT_SEARCH_TERMS = [
    "fat free milk",
    "yogurt",
    "oat milk",
    "fish",
    "bread",
    "soda",
    "cheese",
    "oranges",
]


# ==========================
# Prompting
# ==========================
SYSTEM_PROMPT = """You are a strict data-quality evaluator for a grocery search pipeline.

We sampled (description, categories) rows that were returned for a given search_term.

Your job:
- Decide whether the row is a reasonable match for the search_term.
- Return ONLY one of: PASS, FAIL, UNCLEAR
- PASS = clearly is the thing (or a very close direct variant).
- FAIL = clearly not the thing (adjacent products like "creamer" for coffee, "sauce" for fish, etc).
- UNCLEAR = ambiguous without more info (brand-only, unclear naming, etc).

Be conservative:
- If it's a flavored/adjacent product that isn't the core item, lean FAIL.
- If the item is a mixture/medley, decide based on whether it still reasonably satisfies the search_term.

Things to consider:
- Do not take flavor in consideration when the search_term doesn't include a flavor.
- For example, if the term is simply "fat free milk", do not reject rows for
  having a description "Dairy Pure™ Milk50™ Fat Free Lactose Free Ultra-Pasteurized Chocolate Skim Milk Bottle".
- Fat free isn't a flavor in this case. So we should be accepting flavored fat free milks.
"""

USER_PROMPT_TEMPLATE = """Evaluate each item for search_term: "{search_term}"

Return JSON ONLY in this exact schema:
{{
  "results": [
    {{
      "idx": <int>,
      "label": "PASS" | "FAIL" | "UNCLEAR",
      "reason": <short string, <= 20 words>
    }},
    ...
  ]
}}

Items:
{items_json}
"""


# ==========================
# BigQuery helpers
# ==========================
def parse_table_id(full_table: str) -> Tuple[str, str, str]:
    """
    Accepts:
      project.dataset.table
    Returns:
      (project, dataset, table)
    """
    parts = full_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid table id '{full_table}'. Expected format: project.dataset.table"
        )
    return parts[0], parts[1], parts[2]


def detect_dataset_location(
    client: bigquery.Client,
    full_table: str,
    fallback_location: Optional[str] = None,
) -> Optional[str]:
    """
    Returns the dataset location for the table's dataset (e.g., 'US', 'EU', 'us-east1').
    If it can't detect, returns fallback_location (possibly None).
    """
    project, dataset, _ = parse_table_id(full_table)
    dataset_id = f"{project}.{dataset}"
    try:
        ds = client.get_dataset(dataset_id)
        return getattr(ds, "location", None) or fallback_location
    except Exception:
        return fallback_location


def fetch_distinct_rows(
    client: bigquery.Client,
    table_id: str,
    search_term: str,
    limit: Optional[int],
    location: Optional[str],
) -> List[Tuple[str, str]]:
    sql = f"""
    SELECT DISTINCT
      description,
      categories
    FROM `{table_id}`
    WHERE search_term = @search_term
    ORDER BY description
    """
    if limit is not None:
        sql += "\nLIMIT @limit"

    params = [bigquery.ScalarQueryParameter("search_term", "STRING", search_term)]
    if limit is not None:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    job = (
        client.query(sql, job_config=job_config, location=location)
        if location
        else client.query(sql, job_config=job_config)
    )
    rows = job.result()

    out: List[Tuple[str, str]] = []
    for r in rows:
        out.append((r.get("description") or "", r.get("categories") or ""))
    return out


# ==========================
# Canonical term loading
# ==========================
def _norm_simple(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def load_all_canonical_terms(canonical_terms_path: Path) -> List[str]:
    """
    Loads canonical_search_terms.yaml and returns a sorted list of normalized terms.
    Expected YAML shape:
      search_terms:
        bucket_key:
          breadth: [...]
          drilldown: [...]
    """
    if not canonical_terms_path.exists():
        raise FileNotFoundError(f"canonical_search_terms.yaml not found at: {canonical_terms_path}")

    with canonical_terms_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    st = cfg.get("search_terms") or {}
    terms_set = set()

    for _, block in (st or {}).items():
        block = block or {}
        for t in (block.get("breadth") or []) + (block.get("drilldown") or []):
            nt = _norm_simple(str(t))
            if nt:
                terms_set.add(nt)

    if not terms_set:
        raise RuntimeError("No search_terms found in canonical_search_terms.yaml (check file format).")

    return sorted(terms_set)


def parse_terms_csv(s: str) -> List[str]:
    out: List[str] = []
    for piece in (s or "").split(","):
        t = _norm_simple(piece)
        if t:
            out.append(t)
    # de-dupe while preserving order
    seen = set()
    deduped: List[str] = []
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        deduped.append(t)
    return deduped


# ==========================
# OpenAI batch classify
# ==========================
def classify_batch(
    oa: OpenAI,
    model: str,
    search_term: str,
    batch_items: List[Dict[str, Any]],
    max_retries: int = 5,
) -> List[Dict[str, Any]]:
    items_json = json.dumps(batch_items, ensure_ascii=False)
    prompt = USER_PROMPT_TEMPLATE.format(search_term=search_term, items_json=items_json)

    for attempt in range(1, max_retries + 1):
        try:
            resp = oa.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )

            text = resp.output_text.strip()
            data = json.loads(text)
            results = data.get("results")
            if not isinstance(results, list):
                raise ValueError("Model JSON missing 'results' list")

            cleaned: List[Dict[str, Any]] = []
            for x in results:
                idx = x.get("idx")
                label = (x.get("label") or "").strip().upper()
                reason = (x.get("reason") or "").strip()

                if label not in {"PASS", "FAIL", "UNCLEAR"}:
                    label = "UNCLEAR"
                    if not reason:
                        reason = "Invalid label returned"
                cleaned.append({"idx": idx, "label": label, "reason": reason})

            return cleaned

        except Exception as e:
            if attempt == max_retries:
                return [
                    {
                        "idx": item["idx"],
                        "label": "UNCLEAR",
                        "reason": f"LLM error: {type(e).__name__}",
                    }
                    for item in batch_items
                ]
            time.sleep(min(2**attempt, 20))

    return [
        {"idx": item["idx"], "label": "UNCLEAR", "reason": "Unknown error"}
        for item in batch_items
    ]


# ==========================
# Output path helper
# ==========================
def make_dated_out_dir(root_out_dir: str) -> str:
    """
    Writes into: <root_out_dir>/eval_output/{yyyy}/{MMM}/{dd}
    Example: ./eval_outputs/eval_output/2026/Feb/01
    """
    now_utc = dt.datetime.now(dt.UTC)
    yyyy = now_utc.strftime("%Y")
    mmm = now_utc.strftime("%b")  # Jan, Feb, Mar...
    dd = now_utc.strftime("%d")
    ts = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
    return os.path.join(root_out_dir, yyyy, mmm, dd, ts)


# ==========================
# Main
# ==========================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="LLM eval: classify (description,categories) per search_term as PASS/FAIL/UNCLEAR."
    )
    parser.add_argument(
        "--model",
        default="gpt-4.1-mini",
        help="OpenAI model to use (default: gpt-4.1-mini)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Rows per LLM call (default: 25)",
    )
    parser.add_argument(
        "--limit-per-term",
        type=int,
        default=1000,
        help="Max rows per term (default: 200)",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Root output directory (default: current directory). Script writes into eval_output/YYYY/MMM/DD under this root.",
    )
    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help=f"BigQuery source table (default: {DEFAULT_SOURCE_TABLE})",
    )
    parser.add_argument(
        "--bq-project",
        default=DEFAULT_BQ_PROJECT,
        help=f"BigQuery project for the client (default: {DEFAULT_BQ_PROJECT})",
    )
    parser.add_argument(
        "--bq-location",
        default="",
        help="Optional: force a BigQuery location (e.g., US, EU, us-east1). "
        "If not provided, the script auto-detects dataset location.",
    )
    parser.add_argument(
        "--canonical-terms-yaml",
        default=str(DEFAULT_CANONICAL_TERMS_PATH),
        help=f"Path to canonical_search_terms.yaml (default: {DEFAULT_CANONICAL_TERMS_PATH})",
    )
    parser.add_argument(
        "--all-terms",
        action="store_true",
        help="Run eval for ALL canonical search terms from canonical_search_terms.yaml.",
    )
    parser.add_argument(
        "--terms",
        default="",
        help='Comma-separated list of search_terms to eval (overrides default list). Example: --terms "fish, yogurt, fat free milk"',
    )
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is not set in the environment.")

    # Decide which terms to run
    canonical_terms_path = Path(args.canonical_terms_yaml).expanduser().resolve()
    if args.all_terms:
        terms_to_run = load_all_canonical_terms(canonical_terms_path)
    else:
        cli_terms = parse_terms_csv(args.terms)
        if cli_terms:
            terms_to_run = cli_terms
        else:
            # fallback to the small list (useful for quick testing)
            terms_to_run = [_norm_simple(t) for t in DEFAULT_SEARCH_TERMS if _norm_simple(t)]

    if not terms_to_run:
        raise SystemExit("No search_terms to run. Use --all-terms or --terms.")

    # Create dated output directory
    dated_out_dir = make_dated_out_dir(args.out_dir)
    os.makedirs(dated_out_dir, exist_ok=True)

    # Time-stamped filenames inside dated directory
    ts = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
    out_csv = os.path.join(dated_out_dir, f"llm_eval_false_positives_{ts}.csv")
    out_jsonl = os.path.join(dated_out_dir, f"llm_eval_false_positives_{ts}.jsonl")

    bq = bigquery.Client(project=args.bq_project)
    oa = OpenAI()

    # Determine location
    forced_location = (args.bq_location or "").strip() or None
    detected_location = detect_dataset_location(
        bq, args.source_table, fallback_location=forced_location
    )

    print("Eval configuration:")
    print(f"  source_table: {args.source_table}")
    print(f"  bq_project:   {args.bq_project}")
    print(f"  location:     {detected_location or '(none)'}  (none = let BigQuery decide)")
    print(f"  model:        {args.model}")
    print(f"  batch_size:   {args.batch_size}")
    print(f"  limit/term:   {args.limit_per_term}")
    print(f"  out_root:     {args.out_dir}")
    print(f"  out_dir:      {dated_out_dir}")
    print(f"  terms:        {len(terms_to_run)}")
    if args.all_terms:
        print(f"  terms_source: ALL from {canonical_terms_path}")
    elif args.terms.strip():
        print("  terms_source: --terms")
    else:
        print("  terms_source: DEFAULT_SEARCH_TERMS")
    print()

    all_rows_written = 0
    terms_with_zero_rows = 0

    with open(out_csv, "w", newline="", encoding="utf-8") as f_csv, open(
        out_jsonl, "w", encoding="utf-8"
    ) as f_jsonl:
        writer = csv.DictWriter(
            f_csv,
            fieldnames=[
                "search_term",
                "idx",
                "label",
                "reason",
                "description",
                "categories",
            ],
        )
        writer.writeheader()

        for term in terms_to_run:
            try:
                rows = fetch_distinct_rows(
                    bq,
                    table_id=args.source_table,
                    search_term=term,
                    limit=args.limit_per_term,
                    location=detected_location,
                )
            except NotFound as e:
                raise SystemExit(
                    "BigQuery table not found. Double-check --source-table.\n"
                    f"Current: {args.source_table}\n\n"
                    f"Original error: {e}"
                )

            if not rows:
                terms_with_zero_rows += 1
                print(f"[{term}] distinct rows: 0 (skipping)")
                continue

            print(f"[{term}] distinct rows: {len(rows)}")

            items = [
                {"idx": i, "description": desc, "categories": cats}
                for i, (desc, cats) in enumerate(rows)
            ]

            for start in range(0, len(items), args.batch_size):
                batch = items[start : start + args.batch_size]
                decisions = classify_batch(oa, args.model, term, batch)

                decision_by_idx = {d["idx"]: d for d in decisions if "idx" in d}

                for item in batch:
                    d = decision_by_idx.get(
                        item["idx"],
                        {"label": "UNCLEAR", "reason": "Missing idx in model output"},
                    )
                    row_out = {
                        "search_term": term,
                        "idx": item["idx"],
                        "label": d.get("label", "UNCLEAR"),
                        "reason": d.get("reason", ""),
                        "description": item["description"],
                        "categories": item["categories"],
                    }
                    writer.writerow(row_out)
                    f_jsonl.write(json.dumps(row_out, ensure_ascii=False) + "\n")
                    all_rows_written += 1

                time.sleep(0.2)

    print(f"\n✅ Done. Wrote {all_rows_written} rows")
    print(f"Zero-row terms skipped: {terms_with_zero_rows}")
    print(f"CSV:   {out_csv}")
    print(f"JSONL: {out_jsonl}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())