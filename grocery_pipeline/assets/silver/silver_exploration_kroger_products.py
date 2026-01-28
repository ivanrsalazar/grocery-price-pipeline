import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import yaml
from dagster import AssetExecutionContext, AssetKey, asset

# =====================================================
# BigQuery tables
# =====================================================

BQ_PROJECT = "grocery-pipe-line"

BRONZE_TABLE = f"{BQ_PROJECT}.grocery_bronze.exploration_kroger_products"
SILVER_TABLE = f"{BQ_PROJECT}.grocery_silver.exploration_kroger_products"

# =====================================================
# Config paths
# =====================================================

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
SEARCH_TERMS_PATH = CONFIG_DIR / "canonical_search_terms.yaml"
CATEGORIES_PATH = CONFIG_DIR / "canonical_categories.yaml"

# =====================================================
# Small helpers
# =====================================================


def _norm_simple(s: Optional[str]) -> str:
    """Normalization for search_term keys (lowercase + collapse whitespace)."""
    if not s:
        return ""
    s = s.strip().lower()
    return re.sub(r"\s+", " ", s)


def _require_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing required config file: {p}")


def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return str(obj)


# =====================================================
# Config loading
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
# Rules (only overrides for the two abnormal terms)
# =====================================================


def load_search_term_rules() -> Dict[str, Dict]:
    """
    Returns rules keyed by normalized search_term.

    Only two overrides:
      - packaged meals: match ANY of ["meal", "microwave", "dinner", "skillet", "compleats", "helper"]
      - pet shampoo: match ANY of ["dog shampoo", "cat shampoo", "pet shampoo", "puppy shampoo", "kitten shampoo"]

    Everything else defaults to all_of (tokens from search_term must appear).
    """
    return {
        "packaged meals": {
            "strategy": "any_of",
            "any_of": [
                "meal",
                "microwave",
                "dinner",
                "skillet",
                "compleats",
                "helper",
            ],
        },
        "pet shampoo": {
            "strategy": "any_of",
            "any_of": [
                "dog shampoo",
                "cat shampoo",
                "pet shampoo",
                "puppy shampoo",
                "kitten shampoo",
            ],
        },
    }


def build_term_records() -> List[Tuple[str, str, str, str, List[str]]]:
    """
    Produces list of tuples:
      (search_term_norm, bucket_key, expected_category, strategy, any_of_tokens)

    strategy in {"all_of", "any_of"}
    any_of_tokens used only when strategy == "any_of"
    """
    term_to_bucket = load_search_terms_bucket_map()
    bucket_to_expected = load_category_expected()
    rules = load_search_term_rules()

    records: List[Tuple[str, str, str, str, List[str]]] = []
    missing_buckets = set()

    for term_norm, bucket in sorted(term_to_bucket.items(), key=lambda x: (x[1], x[0])):
        if bucket not in bucket_to_expected:
            missing_buckets.add(bucket)
            continue

        rule = rules.get(term_norm, {})
        strategy = rule.get("strategy", "all_of")

        if strategy not in {"all_of", "any_of"}:
            raise ValueError(f"Invalid strategy '{strategy}' for term '{term_norm}'")

        any_of_tokens: List[str] = []
        if strategy == "any_of":
            any_of_tokens = [
                str(x).strip().lower()
                for x in (rule.get("any_of") or [])
                if str(x).strip()
            ]
            if not any_of_tokens:
                raise ValueError(f"strategy=any_of requires non-empty any_of list for term '{term_norm}'")

        records.append((term_norm, bucket, bucket_to_expected[bucket], strategy, any_of_tokens))

    if missing_buckets:
        raise ValueError(
            "canonical_search_terms.yaml contains buckets not found in canonical_categories.yaml: "
            + ", ".join(sorted(missing_buckets))
        )

    if not records:
        raise RuntimeError("No canonical term records built (check yaml contents).")

    return records


# =====================================================
# Asset
# =====================================================


@asset(
    name="silver_exploration_kroger_products",
    compute_kind="bigquery",
    deps=[AssetKey("exploration_kroger_products_daily")],  # adjust if your bronze asset key differs
)
def silver_exploration_kroger_products(context: AssetExecutionContext) -> None:
    """
    Reads bronze exploration rows and writes filtered silver rows.

    Filters enforced:
      - category matches expected_category (from canonical_categories.yaml)
      - description matching:
          * default: ALL tokens from search_term must appear in description_norm
          * overrides for two terms: ANY OF configured synonyms must appear (EXISTS)

    IMPORTANT: DEDUPES source rows before MERGE so BigQuery never sees >1 source row for the MERGE key:
      (snapshot_date, location_id, search_term, product_id)
    """
    # Prefer resource if you wired it, else create client
    bq = getattr(context.resources, "bigquery_client", None)
    if bq is None:
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)

    term_records = build_term_records()
    context.log.info(
        "Loaded canonical config | "
        f"terms={len(term_records)} | "
        f"search_terms={SEARCH_TERMS_PATH} | categories={CATEGORIES_PATH}"
    )

    # IMPORTANT: every STRUCT in the UNNEST array must have the exact same schema
    # so we always include match_strategy + any_of_tokens (possibly empty array).
    term_map_sql = ",\n".join(
        [
            "STRUCT("
            f"{repr(term)} AS search_term_norm, "
            f"{repr(bucket)} AS bucket_key, "
            f"{repr(expected_cat)} AS expected_category, "
            f"{repr(strategy)} AS match_strategy, "
            + (
                "CAST([" + ", ".join(repr(t) for t in any_of_tokens) + "] AS ARRAY<STRING>)"
                if any_of_tokens
                else "CAST([] AS ARRAY<STRING>)"
            )
            + " AS any_of_tokens"
            ")"
            for term, bucket, expected_cat, strategy, any_of_tokens in term_records
        ]
    )

    # NOTE:
    # - any_of uses EXISTS (non-scalar)
    # - all_of uses COUNTIF vs ARRAY_LENGTH (non-scalar)
    # - final SELECT uses QUALIFY to dedupe source rows for MERGE
    query = f"""
    MERGE `{SILVER_TABLE}` T
    USING (
      WITH term_map AS (
        SELECT * FROM UNNEST([
          {term_map_sql}
        ])
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

          b.raw,  -- keep JSON as JSON

          tm.bucket_key,
          tm.expected_category AS canonical_category,
          tm.match_strategy,
          tm.any_of_tokens,

          -- normalize fields used for filtering + debugging columns
          REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " ") AS description_norm,

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

          ARRAY(
            SELECT REGEXP_REPLACE(LOWER(TRIM(cat)), r"[^a-z0-9]+", " ")
            FROM UNNEST(SPLIT(COALESCE(b.categories, ""), ",")) cat
            WHERE TRIM(cat) != ""
          ) AS categories_norm,

          REGEXP_REPLACE(LOWER(tm.expected_category), r"[^a-z0-9]+", " ") AS expected_category_norm

        FROM `{BRONZE_TABLE}` b
        JOIN term_map tm
          ON REGEXP_REPLACE(LOWER(TRIM(b.search_term)), r"\\s+", " ") = tm.search_term_norm
      ),
      scored AS (
        SELECT
          *,

          -- category match: expected category appears in normalized categories array
          expected_category_norm IN UNNEST(categories_norm) AS matched_category,

          -- description match using strategy:
          CASE
            WHEN match_strategy = "any_of" THEN
              EXISTS (
                SELECT 1
                FROM UNNEST(any_of_tokens) tok
                WHERE tok IS NOT NULL
                  AND tok != ""
                  AND STRPOS(description_norm, REGEXP_REPLACE(LOWER(tok), r"[^a-z0-9]+", " ")) > 0
              )
            ELSE
              (
                ARRAY_LENGTH(term_tokens) = 0
                OR (
                  (SELECT COUNTIF(STRPOS(description_norm, tok) > 0) FROM UNNEST(term_tokens) tok)
                  = ARRAY_LENGTH(term_tokens)
                )
              )
          END AS matched_all_tokens
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

        matched_all_tokens,
        matched_category,

        (matched_all_tokens AND matched_category) AS kept,

        raw,

        canonical_category,
        bucket_key
      FROM scored
      WHERE (matched_all_tokens AND matched_category)

      -- CRITICAL: ensure S has only 1 row per MERGE key
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

    context.log.info(f"Running silver merge into `{SILVER_TABLE}` from `{BRONZE_TABLE}`")
    job = bq.query(query, location="us-east1")
    job.result()
    context.log.info("Silver exploration merge completed.")