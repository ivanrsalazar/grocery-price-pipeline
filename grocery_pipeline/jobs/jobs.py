from dagster import define_asset_job, AssetSelection, multiprocess_executor

_PIPELINE_LOCK_TAGS = {"pipeline_lock": "ec2_exclusive"}

_RETAILERS = ["heb", "meijer", "walmart", "vons", "wd", "sprouts"]

# Job that materializes the daily Kroger bronze asset (API-only, no lock needed)
bronze_kroger_products_daily_job = define_asset_job(
    name="bronze_kroger_products_daily_job",
    selection=AssetSelection.assets(
        "exploration_kroger_products_daily",
        "silver_exploration_kroger_products",
    ),
)

# Job that materializes all 6 retailer scraper bronze assets
bronze_scraper_products_daily_job = define_asset_job(
    name="bronze_scraper_products_daily_job",
    selection=AssetSelection.assets(
        "bronze_heb_products_daily",
        "bronze_meijer_products_daily",
        "bronze_walmart_products_daily",
        "bronze_vons_products_daily",
        "bronze_wd_products_daily",
        "bronze_sprouts_products_daily",
    ),
    executor_def=multiprocess_executor,
    tags=_PIPELINE_LOCK_TAGS,
)

# Per-retailer jobs for sensor-driven runs
per_retailer_jobs = {}
for _r in _RETAILERS:
    _job = define_asset_job(
        name=f"bronze_{_r}_products_daily_job",
        selection=AssetSelection.assets(f"bronze_{_r}_products_daily"),
        tags=_PIPELINE_LOCK_TAGS,
    )
    per_retailer_jobs[_r] = _job

# Convenience exports
bronze_heb_products_daily_job = per_retailer_jobs["heb"]
bronze_meijer_products_daily_job = per_retailer_jobs["meijer"]
bronze_walmart_products_daily_job = per_retailer_jobs["walmart"]
bronze_vons_products_daily_job = per_retailer_jobs["vons"]
bronze_wd_products_daily_job = per_retailer_jobs["wd"]
bronze_sprouts_products_daily_job = per_retailer_jobs["sprouts"]
