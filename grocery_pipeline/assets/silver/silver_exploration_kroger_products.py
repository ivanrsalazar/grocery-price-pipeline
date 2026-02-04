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
SEARCH_TERM_RULES_PATH = CONFIG_DIR / "canonical_search_term_rules.yaml"

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

def _norm_token_for_sql(tok: str) -> str:
    """
    Normalize a token to match the SQL normalization used for description_norm:
      - lowercase
      - non-alnum -> space
      - collapse whitespace
    """
    t = str(tok).strip().lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

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
# Rules
# =====================================================

def load_search_term_rules() -> Tuple[str, Dict[str, Dict]]:
    """
    Load search term matching rules from canonical_search_term_rules.yaml.

    Supported strategies:
      - all_tokens       (default): all tokens from search_term must appear in description_norm
      - any_of           (legacy): ANY of any_of tokens must appear in description_norm
      - require_any_of   (new): require ANY of require_any_of tokens must appear in description_norm
      - require_all_of   (new): require ALL of require_all_of tokens must appear in description_norm
      - none_of          (new): require NONE of none_of tokens appear in description_norm

    Optional lists that may exist on any rule:
      - any_of
      - require_any_of
      - require_all_of
      - none_of
      - strip_tokens  (phrases to remove from description_norm_raw before matching)

    YAML shape (example):

    defaults:
      strategy: all_tokens

    rules:
      coffee:
        strategy: all_tokens
        none_of: ["creamer", "mate"]

      packaged meals:
        strategy: all_tokens
        require_any_of: ["microwave", "skillet"]
        none_of: ["dog", "cat"]   # silly example

    Returns:
      default_strategy (str)
      rules_by_term (dict[str, dict])
        where dict has:
          {
            "strategy": str,
            "any_of": list[str],
            "require_any_of": list[str],
            "require_all_of": list[str],
            "none_of": list[str],
            "strip_tokens": list[str],
          }
    """
    _require_file(SEARCH_TERM_RULES_PATH)

    with SEARCH_TERM_RULES_PATH.open("r") as f:
        cfg = yaml.safe_load(f) or {}

    defaults = cfg.get("defaults") or {}
    default_strategy = str(defaults.get("strategy", "all_tokens")).strip().lower()

    valid_strategies = {"all_tokens", "any_of", "require_any_of", "require_all_of", "none_of"}
    if default_strategy not in valid_strategies:
        raise ValueError(
            f"Invalid defaults.strategy '{default_strategy}' in {SEARCH_TERM_RULES_PATH}. "
            f"Valid: {sorted(valid_strategies)}"
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
        if strategy not in valid_strategies:
            raise ValueError(
                f"Invalid strategy '{strategy}' for term '{term}' in {SEARCH_TERM_RULES_PATH}. "
                f"Valid: {sorted(valid_strategies)}"
            )

        any_of_norm = _norm_list(rule.get("any_of"))
        req_any_norm = _norm_list(rule.get("require_any_of"))
        req_all_norm = _norm_list(rule.get("require_all_of"))
        none_of_norm = _norm_list(rule.get("none_of"))

        # stripping: keep as normalized phrases
        strip_norm = _norm_list(rule.get("strip_tokens"))

        # Basic validations for the "driving" strategy
        if strategy == "any_of" and not any_of_norm:
            raise ValueError(
                f"strategy=any_of requires non-empty any_of list for term '{term}' in {SEARCH_TERM_RULES_PATH}"
            )
        if strategy == "require_any_of" and not req_any_norm:
            raise ValueError(
                f"strategy=require_any_of requires non-empty require_any_of list for term '{term}' "
                f"in {SEARCH_TERM_RULES_PATH}"
            )
        if strategy == "require_all_of" and not req_all_norm:
            raise ValueError(
                f"strategy=require_all_of requires non-empty require_all_of list for term '{term}' "
                f"in {SEARCH_TERM_RULES_PATH}"
            )
        if strategy == "none_of" and not none_of_norm:
            raise ValueError(
                f"strategy=none_of requires non-empty none_of list for term '{term}' in {SEARCH_TERM_RULES_PATH}"
            )

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
    Produces list of tuples:
      (
        search_term_norm,
        bucket_key,
        expected_category,
        strategy,
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

    valid_strategies = {"all_tokens", "any_of", "require_any_of", "require_all_of", "none_of"}

    records: List[Tuple[str, str, str, str, List[str], List[str], List[str], List[str], List[str]]] = []
    missing_buckets = set()

    for term_norm, bucket in sorted(term_to_bucket.items(), key=lambda x: (x[1], x[0])):
        if bucket not in bucket_to_expected:
            missing_buckets.add(bucket)
            continue

        rule = rules_by_term.get(term_norm, {})
        strategy = str(rule.get("strategy", default_strategy)).strip().lower()
        if strategy not in valid_strategies:
            raise ValueError(f"Invalid strategy '{strategy}' for term '{term_norm}'")

        any_of_tokens = list(rule.get("any_of") or [])
        require_any_of_tokens = list(rule.get("require_any_of") or [])
        require_all_of_tokens = list(rule.get("require_all_of") or [])
        none_of_tokens = list(rule.get("none_of") or [])
        strip_tokens = list(rule.get("strip_tokens") or [])

        records.append(
            (
                term_norm,
                bucket,
                bucket_to_expected[bucket],
                strategy,
                any_of_tokens,
                require_any_of_tokens,
                require_all_of_tokens,
                none_of_tokens,
                strip_tokens,
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
    Reads bronze exploration rows and writes filtered silver rows.

    Matching:
      - category match uses categories_norm_str substring match (avoids comma-splitting bug)
      - description match supports:
          * all_tokens (default)
          * any_of (legacy)
          * require_any_of
          * require_all_of
          * none_of
        Plus optional "none_of" can be applied alongside other strategies (if present in YAML rule).
    """
    bq = getattr(context.resources, "bigquery_client", None)
    if bq is None:
        from google.cloud import bigquery
        bq = bigquery.Client(project=BQ_PROJECT)

    term_records = build_term_records()
    context.log.info(
        "Loaded canonical config | "
        f"terms={len(term_records)} | "
        f"search_terms={SEARCH_TERMS_PATH} | categories={CATEGORIES_PATH} | rules={SEARCH_TERM_RULES_PATH}"
    )

    def _arr_sql(items: List[str]) -> str:
        # Always force ARRAY<STRING> type for UNNEST() array-of-struct uniformity
        if not items:
            return "CAST([] AS ARRAY<STRING>)"
        return "CAST([" + ", ".join(repr(x) for x in items) + "] AS ARRAY<STRING>)"

    # IMPORTANT: every STRUCT in the UNNEST array must have the exact same schema.
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

          -- Build strip_pattern from strip_tokens
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

          -- Keep if you want to store it (but don't use it for matching)
          ARRAY(
            SELECT REGEXP_REPLACE(LOWER(TRIM(cat)), r"[^a-z0-9]+", " ")
            FROM UNNEST(SPLIT(COALESCE(b.categories, ""), ",")) cat
            WHERE TRIM(cat) != ""
          ) AS categories_norm,

          -- Category matching uses WHOLE string (categories can contain commas inside one label)
          REGEXP_REPLACE(LOWER(COALESCE(b.categories, "")), r"[^a-z0-9]+", " ") AS categories_norm_str,
          REGEXP_REPLACE(LOWER(tm.expected_category), r"[^a-z0-9]+", " ") AS expected_category_norm

        FROM `{BRONZE_TABLE}` b
        JOIN term_map tm
          ON REGEXP_REPLACE(LOWER(TRIM(b.search_term)), r"\\s+", " ") = tm.search_term_norm
      ),
      scored AS (
        SELECT
          *,

          -- category match: expected category appears as a substring in the normalized category string
          STRPOS(categories_norm_str, expected_category_norm) > 0 AS matched_category,

          -- base description match driven by strategy
          CASE
            WHEN match_strategy = "any_of" THEN
              EXISTS (
                SELECT 1
                FROM UNNEST(any_of_tokens) tok
                WHERE tok IS NOT NULL
                  AND tok != ""
                  AND STRPOS(description_norm, tok) > 0
              )

            WHEN match_strategy = "require_any_of" THEN
              EXISTS (
                SELECT 1
                FROM UNNEST(require_any_of_tokens) tok
                WHERE tok IS NOT NULL
                  AND tok != ""
                  AND STRPOS(description_norm, tok) > 0
              )

            WHEN match_strategy = "require_all_of" THEN
              (
                ARRAY_LENGTH(require_all_of_tokens) > 0
                AND (
                  SELECT COUNTIF(STRPOS(description_norm, tok) > 0)
                  FROM UNNEST(require_all_of_tokens) tok
                ) = ARRAY_LENGTH(require_all_of_tokens)
              )

            WHEN match_strategy = "none_of" THEN
              NOT EXISTS (
                SELECT 1
                FROM UNNEST(none_of_tokens) tok
                WHERE tok IS NOT NULL
                  AND tok != ""
                  AND STRPOS(description_norm, tok) > 0
              )

            ELSE
              (
                ARRAY_LENGTH(term_tokens) = 0
                OR (
                  (SELECT COUNTIF(STRPOS(description_norm, tok) > 0) FROM UNNEST(term_tokens) tok)
                  = ARRAY_LENGTH(term_tokens)
                )
              )
          END AS matched_all_tokens,

          -- optional "none_of" gate that can apply in addition to the base strategy
          NOT EXISTS (
            SELECT 1
            FROM UNNEST(none_of_tokens) tok
            WHERE tok IS NOT NULL
              AND tok != ""
              AND STRPOS(description_norm, tok) > 0
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

        matched_all_tokens,
        matched_category,

        (matched_all_tokens AND matched_category AND passed_none_of) AS kept,

        raw,

        canonical_category,
        bucket_key
      FROM scored
      WHERE (matched_all_tokens AND matched_category AND passed_none_of)

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