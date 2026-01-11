from dagster import define_asset_job, AssetSelection

# Job that materializes the daily Kroger bronze asset
bronze_kroger_products_daily_job = define_asset_job(
    name="bronze_kroger_products_daily_job",
    selection=AssetSelection.assets(
        "bronze_kroger_products_daily"
    ),
)