from dagster import define_asset_job, AssetSelection

# Job that materializes the daily Kroger bronze asset
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
)