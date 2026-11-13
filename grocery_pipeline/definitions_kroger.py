"""Self-contained Dagster code location for the Kroger-only pipeline."""

from dagster import Definitions

from grocery_pipeline.assets.bronze.bronze_exploration_daily import (
    exploration_kroger_products_daily,
)
from grocery_pipeline.assets.silver.silver_exploration_kroger_products import (
    silver_exploration_kroger_products,
)
from grocery_pipeline.assets.gold.dbt_gold import gold_dbt_models
from grocery_pipeline.jobs.jobs import bronze_kroger_products_daily_job
from grocery_pipeline.resources.bigquery import bigquery_client
from grocery_pipeline.schedules.schedules import bronze_kroger_products_daily_schedule


defs = Definitions(
    assets=[
        exploration_kroger_products_daily,
        silver_exploration_kroger_products,
        gold_dbt_models,
    ],
    jobs=[bronze_kroger_products_daily_job],
    schedules=[bronze_kroger_products_daily_schedule],
    resources={"bigquery_client": bigquery_client},
)
