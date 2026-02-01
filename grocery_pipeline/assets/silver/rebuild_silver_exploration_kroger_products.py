import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

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
# Config paths
# =====================================================

# This script lives at: grocery_pipeline/assets/silver/<this_file>.py
# parents[2] -> grocery_pipeline/
CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

SEARCH_TERMS_PATH = CONFIG_DIR / "canonical_search_terms.yaml"
CATEGORIES_PATH = CONFIG_DIR / "canonical_categories.yaml"
SEARCH_TERM_RULES_PATH = CONFIG_DIR / "canonical_search_term_rules.yaml"


# =====================================================
# Helpers
# =====================================================

def _require_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing required config file: {p}")

def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return str(obj)

def _norm_simple(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_token_for_sql(tok: str) -> str:
    """
    Normalize token to match SQL normalization used for description_norm:
      - lowercase
      - non-alnum -> space
      - collapse whitespace
    """
    t = str(tok).strip().lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# =====================================================
# Load canonical search terms + categories
# =====================================================

def load_search_terms_bucket_map() -> Dict[str, str]:
    """
    Returns {normalized_search_term: bucket_key} from canonical_search_terms.yaml.
    """
    _require_file(SEARCH_TERMS_PATH)
    with SEARCH_TERMS_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    st = cfg.get("search_terms") or {}
    mapping: Dict[str, str] = {}

    for bucket_key, block in (st or {}).items():
        block = block or {}
        terms = (block.get("breadth") or []) + (block.get("drilldown") or [])
        for term in terms:
            t = _norm_simple(term)
            if t:
                mapping[t] = bucket_key  # last-write wins

    if not mapping:
        raise RuntimeError("No search_terms found in canonical_search_terms.yaml")

    return mapping


def load_category_expected() -> Dict[str, str]:
    """
    Returns {bucket_key: expected_category_string} from canonical_categories.yaml
    """
    _require_file(CATEGORIES_PATH)
    with CATEGORIES_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    cats = cfg.get("categories") or {}
    out: Dict[str, str] = {}

    for bucket_key, block in (cats or {}).items():
        block = block or {}
        expected_category = block.get("expected_category")
        if not expected_category:
            raise ValueError(
                f"canonical_categories.yaml missing expected_category for bucket '{bucket_key}'"
            )
        out[bucket_key] = str(expected_category)

    if not out:
        raise RuntimeError("No categories found in canonical_categories.yaml")

    return out


# =====================================================
# Load rules (supports strategies + optional none_of/strip)
# =====================================================

def load_search_term_rules() -> Tuple[str, Dict[str, Dict]]:
    """
    Loads canonical_search_term_rules.yaml.

    Supported strategies:
      - all_tokens (default)
      - any_of
      - require_any_of
      - require_all_of
      - none_of

    Optional lists that may exist on any rule:
      - any_of
      - require_any_of
      - require_all_of
      - none_of
      - strip_tokens  (phrases to remove from description_norm_raw before matching)

    Returns:
      default_strategy, rules_by_term
    """
    _require_file(SEARCH_TERM_RULES_PATH)
    with SEARCH_TERM_RULES_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    defaults = cfg.get("defaults") or {}
    default_strategy = str(defaults.get("strategy", "all_tokens")).strip().lower()

    valid = {"all_tokens", "any_of", "require_any_of", "require_all_of", "none_of"}
    if default_strategy not in valid:
        raise ValueError(
            f"Invalid defaults.strategy '{default_strategy}' in {SEARCH_TERM_RULES_PATH}. "
            f"Valid: {sorted(valid)}"
        )

    raw_rules = cfg.get("rules") or {}
    rules_by_term: Dict[str, Dict] = {}

    def _norm_list(val) -> List[str]:
        out: List[str] = []
        for x in (val or []):
            t = _norm_token_for_sql(str(x))
            if t:
                out.append(t)
        return out

    for term, rule in raw_rules.items():
        term_norm = _norm_simple(term)
        if not term_norm:
            continue

        rule = rule or {}
        strategy = str(rule.get("strategy", default_strategy)).strip().lower()
        if strategy not in valid:
            raise ValueError(
                f"Invalid strategy '{strategy}' for term '{term}' in {SEARCH_TERM_RULES_PATH}. "
                f"Valid: {sorted(valid)}"
            )

        any_of_norm = _norm_list(rule.get("any_of"))
        req_any_norm = _norm_list(rule.get("require_any_of"))
        req_all_norm = _norm_list(rule.get("require_all_of"))
        none_of_norm = _norm_list(rule.get("none_of"))

        # stripping: keep as normalized phrases
        strip_norm = _norm_list(rule.get("strip_tokens"))

        # driving strategy validations
        if strategy == "any_of" and not any_of_norm:
            raise ValueError(f"strategy=any_of requires any_of for term '{term}'")
        if strategy == "require_any_of" and not req_any_norm:
            raise ValueError(f"strategy=require_any_of requires require_any_of for term '{term}'")
        if strategy == "require_all_of" and not req_all_norm:
            raise ValueError(f"strategy=require_all_of requires require_all_of for term '{term}'")
        if strategy == "none_of" and not none_of_norm:
            raise ValueError(f"strategy=none_of requires none_of for term '{term}'")

        rules_by_term[term_norm] = {
            "strategy": strategy,
            "any_of": any_of_norm,
            "require_any_of": req_any_norm,
            "require_all_of": req_all_norm,
            "none_of": none_of_norm,
            "strip_tokens": strip_norm,
        }

    return default_strategy, rules_by_term


def build_term_records() -> List[
    Tuple[str, str, str, str, List[str], List[str], List[str], List[str], List[str]]
]:
    """
    Returns list of:
      (
        search_term_norm,
        bucket_key,
        expected_category,
        match_strategy,
        any_of_tokens,
        require_any_of_tokens,
        require_all_of_tokens,
        none_of_tokens,
        strip_tokens
      )
    """
    term_to_bucket = load_search_terms_bucket_map()
    bucket_to_expected = load_category_expected()
    default_strategy, rules_by_term = load_search_term_rules()

    valid = {"all_tokens", "any_of", "require_any_of", "require_all_of", "none_of"}

    records = []
    missing_buckets = set()

    for term_norm, bucket in sorted(term_to_bucket.items(), key=lambda x: (x[1], x[0])):
        if bucket not in bucket_to_expected:
            missing_buckets.add(bucket)
            continue

        rule = rules_by_term.get(term_norm, {})
        strategy = str(rule.get("strategy", default_strategy)).strip().lower()
        if strategy not in valid:
            raise ValueError(f"Invalid strategy '{strategy}' for term '{term_norm}'")

        records.append(
            (
                term_norm,
                bucket,
                bucket_to_expected[bucket],
                strategy,
                list(rule.get("any_of") or []),
                list(rule.get("require_any_of") or []),
                list(rule.get("require_all_of") or []),
                list(rule.get("none_of") or []),
                list(rule.get("strip_tokens") or []),
            )
        )

    if missing_buckets:
        raise ValueError(
            "canonical_search_terms.yaml contains buckets not found in canonical_categories.yaml: "
            + ", ".join(sorted(missing_buckets))
        )

    if not records:
        raise RuntimeError("No canonical term records built (check yaml contents).")

    return records


# =====================================================
# Main rebuild
# =====================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild grocery_silver.exploration_kroger_products from grocery_bronze.exploration_kroger_products"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Only process bronze rows with snapshot_date >= CURRENT_DATE() - lookback_days (default: 14). Ignored if --all.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process ALL bronze rows (no lookback filter).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry-run the query (validate only, no execution).",
    )

    args = parser.parse_args()

    term_records = build_term_records()
    print(
        "Loaded configs:\n"
        f"  terms: {len(term_records)}\n"
        f"  search_terms: {SEARCH_TERMS_PATH}\n"
        f"  categories:  {CATEGORIES_PATH}\n"
        f"  rules:       {SEARCH_TERM_RULES_PATH}"
    )

    def _arr_sql(items: List[str]) -> str:
        if not items:
            return "CAST([] AS ARRAY<STRING>)"
        return "CAST([" + ", ".join(repr(x) for x in items) + "] AS ARRAY<STRING>)"

    term_map_sql = ",\n".join(
        [
            "STRUCT("
            f"{repr(term)} AS search_term_norm, "
            f"{repr(bucket)} AS bucket_key, "
            f"{repr(expected_cat)} AS expected_category, "
            f"{repr(strategy)} AS match_strategy, "
            f"{_arr_sql(any_of_tokens)} AS any_of_tokens, "
            f"{_arr_sql(require_any_of_tokens)} AS require_any_of_tokens, "
            f"{_arr_sql(require_all_of_tokens)} AS require_all_of_tokens, "
            f"{_arr_sql(none_of_tokens)} AS none_of_tokens, "
            f"{_arr_sql(strip_tokens)} AS strip_tokens"
            ")"
            for (
                term,
                bucket,
                expected_cat,
                strategy,
                any_of_tokens,
                require_any_of_tokens,
                require_all_of_tokens,
                none_of_tokens,
                strip_tokens,
            ) in term_records
        ]
    )

    lookback_clause = ""
    if not args.all:
        if args.lookback_days <= 0:
            raise ValueError("--lookback-days must be > 0 (or use --all).")
        lookback_clause = f"\n        WHERE b.snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {args.lookback_days} DAY)\n"

    query = f"""
    MERGE `{SILVER_TABLE}` T
    USING (
      WITH term_map AS (
        SELECT * FROM UNNEST([
          {term_map_sql}
        ])
      ),
      bronze_filtered AS (
        SELECT *
        FROM `{BRONZE_TABLE}` b
        {lookback_clause}
      ),
      src AS (
        SELECT
          b.snapshot_date,
          b.search_term,
          b.location_id,
          b.ingestion_ts,

          b.product_id,
          b.upc,

          b.brand,
          b.categories,
          b.country_of_origin,
          b.description,
          b.receipt_description,

          CAST(b.snap_eligible AS BOOL) AS snap_eligible,
          CAST(b.price AS FLOAT64) AS price,

          b.size,
          b.sold_by,
          b.gross_weight,
          b.net_weight,
          b.temperature,
          b.ingredient_statement,

          b.raw,

          tm.bucket_key,
          tm.expected_category AS canonical_category,
          tm.match_strategy,
          tm.any_of_tokens,
          tm.require_any_of_tokens,
          tm.require_all_of_tokens,
          tm.none_of_tokens,
          tm.strip_tokens,

          -- normalize description (raw)
          REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " ") AS description_norm_raw,

          -- FIX: build strip_pattern without referencing any non-aggregated columns alongside STRING_AGG
          (
            SELECT
              IF(
                COUNT(1) = 0,
                NULL,
                CONCAT(
                  r"(^|\\s)(",
                  STRING_AGG(
                    REPLACE(
                      REGEXP_REPLACE(LOWER(tok), r"[^a-z0-9]+", " "),
                      " ",
                      r"\\s+"
                    ),
                    "|"
                  ),
                  r")(\\s|$)"
                )
              )
            FROM UNNEST(tm.strip_tokens) tok
            WHERE tok IS NOT NULL AND tok != ""
          ) AS strip_pattern,

          -- Apply stripping if strip_pattern exists, then normalize whitespace.
          REGEXP_REPLACE(
            IF(
              (
                SELECT
                  IF(
                    COUNT(1) = 0,
                    NULL,
                    CONCAT(
                      r"(^|\\s)(",
                      STRING_AGG(
                        REPLACE(
                          REGEXP_REPLACE(LOWER(tok), r"[^a-z0-9]+", " "),
                          " ",
                          r"\\s+"
                        ),
                        "|"
                      ),
                      r")(\\s|$)"
                    )
                  )
                FROM UNNEST(tm.strip_tokens) tok
                WHERE tok IS NOT NULL AND tok != ""
              ) IS NULL,
              REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " "),
              REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " "),
                (
                  SELECT
                    CONCAT(
                      r"(^|\\s)(",
                      STRING_AGG(
                        REPLACE(
                          REGEXP_REPLACE(LOWER(tok), r"[^a-z0-9]+", " "),
                          " ",
                          r"\\s+"
                        ),
                        "|"
                      ),
                      r")(\\s|$)"
                    )
                  FROM UNNEST(tm.strip_tokens) tok
                  WHERE tok IS NOT NULL AND tok != ""
                ),
                " "
              )
            ),
            r"\\s+",
            " "
          ) AS description_norm,

          ARRAY(
            SELECT tok
            FROM UNNEST(
              SPLIT(
                REGEXP_REPLACE(LOWER(COALESCE(b.search_term, "")), r"[^a-z0-9]+", " "),
                " "
              )
            ) tok
            WHERE tok IS NOT NULL AND tok != ""
          ) AS term_tokens,

          -- Keep array form if you want it in silver, but do NOT use for matching.
          ARRAY(
            SELECT REGEXP_REPLACE(LOWER(TRIM(cat)), r"[^a-z0-9]+", " ")
            FROM UNNEST(SPLIT(COALESCE(b.categories, ""), ",")) cat
            WHERE TRIM(cat) != ""
          ) AS categories_norm,

          -- Category matching uses WHOLE string (categories can contain commas inside one label)
          REGEXP_REPLACE(LOWER(COALESCE(b.categories, "")), r"[^a-z0-9]+", " ") AS categories_norm_str,
          REGEXP_REPLACE(LOWER(tm.expected_category), r"[^a-z0-9]+", " ") AS expected_category_norm

        FROM bronze_filtered b
        JOIN term_map tm
          ON REGEXP_REPLACE(LOWER(TRIM(b.search_term)), r"\\s+", " ") = tm.search_term_norm
      ),
      scored AS (
        SELECT
          *,

          STRPOS(categories_norm_str, expected_category_norm) > 0 AS matched_category,

          CASE
            WHEN match_strategy = "any_of" THEN
              EXISTS (SELECT 1 FROM UNNEST(any_of_tokens) tok WHERE tok != "" AND STRPOS(description_norm, tok) > 0)

            WHEN match_strategy = "require_any_of" THEN
              EXISTS (SELECT 1 FROM UNNEST(require_any_of_tokens) tok WHERE tok != "" AND STRPOS(description_norm, tok) > 0)

            WHEN match_strategy = "require_all_of" THEN
              (
                ARRAY_LENGTH(require_all_of_tokens) > 0
                AND (SELECT COUNTIF(STRPOS(description_norm, tok) > 0) FROM UNNEST(require_all_of_tokens) tok)
                    = ARRAY_LENGTH(require_all_of_tokens)
              )

            WHEN match_strategy = "none_of" THEN
              NOT EXISTS (SELECT 1 FROM UNNEST(none_of_tokens) tok WHERE tok != "" AND STRPOS(description_norm, tok) > 0)

            ELSE
              (
                ARRAY_LENGTH(term_tokens) = 0
                OR (SELECT COUNTIF(STRPOS(description_norm, tok) > 0) FROM UNNEST(term_tokens) tok)
                    = ARRAY_LENGTH(term_tokens)
              )
          END AS matched_desc,

          NOT EXISTS (
            SELECT 1
            FROM UNNEST(none_of_tokens) tok
            WHERE tok != "" AND STRPOS(description_norm, tok) > 0
          ) AS passed_none_of
        FROM src
      )
      SELECT
        snapshot_date,
        search_term,
        location_id,
        ingestion_ts,

        product_id,
        upc,

        brand,
        categories,
        country_of_origin,
        description,
        receipt_description,
        snap_eligible,
        price,
        size,
        sold_by,
        gross_weight,
        net_weight,
        temperature,
        ingredient_statement,

        term_tokens,
        description_norm,
        categories_norm,

        matched_desc AS matched_all_tokens,
        matched_category,

        (matched_category AND matched_desc AND passed_none_of) AS kept,

        raw,

        canonical_category,
        bucket_key
      FROM scored
      WHERE (matched_category AND matched_desc AND passed_none_of)

      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY snapshot_date, location_id, search_term, product_id
        ORDER BY ingestion_ts DESC
      ) = 1
    ) S
    ON
      T.snapshot_date = S.snapshot_date
      AND T.location_id = S.location_id
      AND T.search_term = S.search_term
      AND T.product_id = S.product_id

    WHEN MATCHED THEN UPDATE SET
      ingestion_ts         = S.ingestion_ts,

      upc                  = S.upc,
      brand                = S.brand,
      categories           = S.categories,
      country_of_origin    = S.country_of_origin,
      description          = S.description,
      receipt_description  = S.receipt_description,
      snap_eligible        = S.snap_eligible,
      price                = S.price,
      size                 = S.size,
      sold_by              = S.sold_by,
      gross_weight         = S.gross_weight,
      net_weight           = S.net_weight,
      temperature          = S.temperature,
      ingredient_statement = S.ingredient_statement,

      term_tokens          = S.term_tokens,
      description_norm     = S.description_norm,
      categories_norm      = S.categories_norm,

      matched_all_tokens   = S.matched_all_tokens,
      matched_category     = S.matched_category,
      kept                 = S.kept,

      raw                  = S.raw,

      canonical_category   = S.canonical_category,
      bucket_key           = S.bucket_key

    WHEN NOT MATCHED THEN INSERT (
      snapshot_date,
      search_term,
      location_id,
      ingestion_ts,

      product_id,
      upc,

      brand,
      categories,
      country_of_origin,
      description,
      receipt_description,
      snap_eligible,
      price,
      size,
      sold_by,
      gross_weight,
      net_weight,
      temperature,
      ingredient_statement,

      term_tokens,
      description_norm,
      categories_norm,

      matched_all_tokens,
      matched_category,
      kept,

      raw,

      canonical_category,
      bucket_key
    ) VALUES (
      S.snapshot_date,
      S.search_term,
      S.location_id,
      S.ingestion_ts,

      S.product_id,
      S.upc,

      S.brand,
      S.categories,
      S.country_of_origin,
      S.description,
      S.receipt_description,
      S.snap_eligible,
      S.price,
      S.size,
      S.sold_by,
      S.gross_weight,
      S.net_weight,
      S.temperature,
      S.ingredient_statement,

      S.term_tokens,
      S.description_norm,
      S.categories_norm,

      S.matched_all_tokens,
      S.matched_category,
      S.kept,

      S.raw,

      S.canonical_category,
      S.bucket_key
    );
    """

    client = bigquery.Client(project=BQ_PROJECT)

    if args.dry_run:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        client.query(query, job_config=job_config, location=BQ_LOCATION)
        print("[DRY RUN] Query validated successfully.")
        return 0

    print(f"Running MERGE: bronze -> silver")
    print(f"  BRONZE: {BRONZE_TABLE}")
    print(f"  SILVER: {SILVER_TABLE}")
    if args.all:
        print("  Mode: ALL bronze rows")
    else:
        print(f"  Mode: lookback {args.lookback_days} days")

    job = client.query(query, location=BQ_LOCATION)
    job.result()

    print("✅ Rebuild MERGE completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())