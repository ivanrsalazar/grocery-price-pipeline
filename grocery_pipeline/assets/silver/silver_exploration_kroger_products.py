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

BQ_LOCATION = "us-east1"
LOOKBACK_DAYS = 7

# =====================================================
# Config paths
# =====================================================

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
SEARCH_TERMS_PATH = CONFIG_DIR / "canonical_search_terms.yaml"
CATEGORIES_PATH = CONFIG_DIR / "canonical_categories.yaml"

# =====================================================
# Helpers
# =====================================================


def _norm_simple(s: Optional[str]) -> str:
    """Normalize search terms so config and bronze match (case/whitespace)."""
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _require_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing required config file: {p}")


# =====================================================
# Config loading
# =====================================================


def load_search_terms_bucket_map() -> Dict[str, str]:
    """
    Returns {normalized_search_term: bucket_key} from canonical_search_terms.yaml
    """
    _require_file(SEARCH_TERMS_PATH)
    with SEARCH_TERMS_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    st = cfg.get("search_terms") or {}
    mapping: Dict[str, str] = {}

    for bucket_key, block in st.items():
        block = block or {}
        terms = (block.get("breadth") or []) + (block.get("drilldown") or [])
        for term in terms:
            t = _norm_simple(term)
            if t:
                mapping[t] = bucket_key  # last-write wins if duplicates

    if not mapping:
        raise RuntimeError(
            f"No search terms found in {SEARCH_TERMS_PATH}. Expected key 'search_terms'."
        )

    return mapping


def load_category_defaults() -> Dict[str, Dict]:
    """
    Returns:
      {bucket_key: {expected_category: str, temperature: str|None, snap: bool|None}}
    from canonical_categories.yaml
    """
    _require_file(CATEGORIES_PATH)
    with CATEGORIES_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    cats = cfg.get("categories") or {}
    out: Dict[str, Dict] = {}

    for bucket_key, block in cats.items():
        block = block or {}
        expected_category = block.get("expected_category")
        defaults = block.get("defaults") or {}
        if not expected_category:
            raise ValueError(
                f"canonical_categories.yaml missing expected_category for bucket '{bucket_key}'"
            )

        out[bucket_key] = {
            "expected_category": str(expected_category),
            "temperature": defaults.get("temperature"),
            "snap": defaults.get("snap"),
        }

    if not out:
        raise RuntimeError(f"No categories found in {CATEGORIES_PATH}.")

    return out


def build_term_records() -> List[Tuple[str, str, str, Optional[str], Optional[bool]]]:
    """
    Produces list of tuples:
      (term_norm, bucket_key, expected_category, expected_temperature, expected_snap)
    """
    term_to_bucket = load_search_terms_bucket_map()
    bucket_to_defaults = load_category_defaults()

    records: List[Tuple[str, str, str, Optional[str], Optional[bool]]] = []
    missing_buckets = set()

    for term_norm, bucket in sorted(term_to_bucket.items(), key=lambda x: (x[1], x[0])):
        if bucket not in bucket_to_defaults:
            missing_buckets.add(bucket)
            continue

        d = bucket_to_defaults[bucket]
        records.append(
            (
                term_norm,
                bucket,
                d["expected_category"],
                d.get("temperature"),
                d.get("snap"),
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
# Asset
# =====================================================


@asset(
    name="silver_exploration_kroger_products",
    compute_kind="bigquery",
    deps=[AssetKey("exploration_kroger_products_daily")],
)
def silver_exploration_kroger_products(context: AssetExecutionContext) -> None:
    """
    Writes to grocery_silver.exploration_kroger_products and aligns with schema:

    - Includes debug columns:
      term_tokens (REPEATED STRING),
      description_norm (STRING),
      categories_norm (REPEATED STRING),
      matched_all_tokens (BOOL),
      matched_category (BOOL),
      kept (BOOL),
      canonical_category (STRING),
      bucket_key (STRING),
      raw (JSON)

    Filtering logic (kept = TRUE):
      - canonical_category must appear in categories_norm
      - all term_tokens must appear in description_norm
      - temperature must match expected (if expected provided)
      - snap_eligible must match expected (if expected provided)
    """
    bq = getattr(context.resources, "bigquery_client", None)
    if bq is None:
        from google.cloud import bigquery

        bq = bigquery.Client(project=BQ_PROJECT)

    term_records = build_term_records()
    context.log.info(
        f"Loaded canonical config | terms={len(term_records)} | "
        f"search_terms={SEARCH_TERMS_PATH} | categories={CATEGORIES_PATH}"
    )

    # UNNEST-able mapping table
    term_map_sql = ",\n".join(
        [
            "STRUCT("
            f"{repr(term_norm)} AS search_term_norm, "
            f"{repr(bucket)} AS bucket_key, "
            f"{repr(expected_cat)} AS expected_category, "
            f"{repr(expected_temp) if expected_temp is not None else 'NULL'} AS expected_temperature, "
            f"{'TRUE' if expected_snap is True else ('FALSE' if expected_snap is False else 'NULL')} AS expected_snap"
            ")"
            for term_norm, bucket, expected_cat, expected_temp, expected_snap in term_records
        ]
    )

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
        FROM `{BRONZE_TABLE}`
        WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
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
          b.snap_eligible,
          b.price,
          b.size,
          b.sold_by,
          b.gross_weight,
          b.net_weight,
          b.temperature,
          b.ingredient_statement,
          b.raw,

          tm.bucket_key AS bucket_key,
          tm.expected_category AS canonical_category,

          -- normalize expected category for matching against categories_norm
          REGEXP_REPLACE(LOWER(tm.expected_category), r"[^a-z0-9]+", " ") AS expected_category_norm,

          -- expected snap
          tm.expected_snap AS expected_snap,

          -- normalize expected temperature
          CASE
            WHEN tm.expected_temperature IS NULL THEN NULL
            WHEN REGEXP_CONTAINS(LOWER(tm.expected_temperature), r"frozen") THEN "FROZEN"
            WHEN REGEXP_CONTAINS(LOWER(tm.expected_temperature), r"refriger") THEN "REFRIGERATED"
            WHEN REGEXP_CONTAINS(LOWER(tm.expected_temperature), r"ambient") THEN "AMBIENT"
            ELSE NULL
          END AS expected_temperature_norm,

          -- normalize observed description (debug col)
          REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " ") AS description_norm,

          -- term tokens (debug col)
          ARRAY(
            SELECT tok
            FROM UNNEST(SPLIT(REGEXP_REPLACE(LOWER(COALESCE(b.search_term, "")), r"[^a-z0-9]+", " "), " ")) tok
            WHERE tok IS NOT NULL AND tok != ""
          ) AS term_tokens,

          -- categories_norm (debug col)
          ARRAY(
            SELECT REGEXP_REPLACE(LOWER(TRIM(cat)), r"[^a-z0-9]+", " ")
            FROM UNNEST(SPLIT(COALESCE(b.categories, ""), ",")) cat
            WHERE TRIM(cat) != ""
          ) AS categories_norm,

          -- normalize observed temperature for matching (not stored in table)
          CASE
            WHEN b.temperature IS NULL THEN NULL
            WHEN REGEXP_CONTAINS(LOWER(b.temperature), r"frozen") THEN "FROZEN"
            WHEN REGEXP_CONTAINS(LOWER(b.temperature), r"refriger") THEN "REFRIGERATED"
            WHEN REGEXP_CONTAINS(LOWER(b.temperature), r"ambient") THEN "AMBIENT"
            ELSE NULL
          END AS obs_temperature_norm

        FROM bronze_filtered b
        JOIN term_map tm
          ON REGEXP_REPLACE(LOWER(COALESCE(b.search_term, "")), r"\\s+", " ") = tm.search_term_norm
      ),
      scored AS (
        SELECT
          *,

          -- expected category must be present in categories_norm
          (expected_category_norm IN UNNEST(categories_norm)) AS matched_category,

          -- all tokens from term_tokens must appear in description_norm
          IFNULL(
            (SELECT LOGICAL_AND(STRPOS(description_norm, tok) > 0) FROM UNNEST(term_tokens) tok),
            FALSE
          ) AS matched_all_tokens,

          -- expected temperature (if provided) must match normalized observed
          IF(expected_temperature_norm IS NULL, TRUE, obs_temperature_norm = expected_temperature_norm) AS matched_temperature,

          -- expected snap (if provided) must match
          IF(expected_snap IS NULL, TRUE, snap_eligible = expected_snap) AS matched_snap
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

        (matched_all_tokens AND matched_category AND matched_temperature AND matched_snap) AS kept,

        raw,
        canonical_category,
        bucket_key
      FROM scored
      WHERE (matched_all_tokens AND matched_category AND matched_temperature AND matched_snap)
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

    context.log.info(
        f"Running silver merge into `{SILVER_TABLE}` from `{BRONZE_TABLE}` "
        f"(lookback_days={LOOKBACK_DAYS}, location={BQ_LOCATION})"
    )
    job = bq.query(query, location=BQ_LOCATION)
    job.result()
    context.log.info("Silver exploration merge completed.")