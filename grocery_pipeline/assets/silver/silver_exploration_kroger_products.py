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

    for bucket_key, block in cats.items():
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


def build_term_records() -> List[Tuple[str, str, str]]:
    """
    Produces list of tuples:
      (normalized_search_term, bucket_key, expected_category)
    """
    term_to_bucket = load_search_terms_bucket_map()
    bucket_to_expected = load_category_expected()

    records: List[Tuple[str, str, str]] = []
    missing_buckets = set()

    for term, bucket in sorted(term_to_bucket.items(), key=lambda x: (x[1], x[0])):
        if bucket not in bucket_to_expected:
            missing_buckets.add(bucket)
            continue
        records.append((term, bucket, bucket_to_expected[bucket]))

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
      - description contains all tokens from search_term
    """
    # Prefer resource if you wired it, else create client
    bq = getattr(context.resources, "bigquery_client", None)
    if bq is None:
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)

    term_records = build_term_records()
    context.log.info(
        f"Loaded canonical config | terms={len(term_records)} | "
        f"search_terms={SEARCH_TERMS_PATH} | categories={CATEGORIES_PATH}"
    )

    # Build UNNEST-able array of STRUCTs for mapping in SQL
    # Use normalized search_term consistently (lowercase + collapsed whitespace)
    term_map_sql = ",\n".join(
        [
            "STRUCT("
            f"{repr(term)} AS search_term_norm, "
            f"{repr(bucket)} AS bucket_key, "
            f"{repr(expected_cat)} AS expected_category"
            ")"
            for term, bucket, expected_cat in term_records
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

          tm.bucket_key,
          tm.expected_category AS canonical_category,

          -- normalize description + category list
          REGEXP_REPLACE(LOWER(COALESCE(b.description, "")), r"[^a-z0-9]+", " ") AS description_norm,

          ARRAY(
            SELECT tok
            FROM UNNEST(SPLIT(REGEXP_REPLACE(LOWER(COALESCE(b.search_term, "")), r"[^a-z0-9]+", " "), " ")) tok
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

          -- description match: all term tokens appear in description_norm
          IFNULL(
            (SELECT LOGICAL_AND(STRPOS(description_norm, tok) > 0) FROM UNNEST(term_tokens) tok),
            FALSE
          ) AS matched_all_tokens
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
    job = bq.query(query)
    job.result()
    context.log.info("Silver exploration merge completed.")