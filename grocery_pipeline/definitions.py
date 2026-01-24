from dagster import Definitions

from grocery_pipeline.assets.bronze.bronze_exploration_daily import (
    exploration_kroger_products_daily,
)
from grocery_pipeline.jobs.jobs import (
    bronze_kroger_products_daily_job,
)
from grocery_pipeline.schedules.schedules import (
    bronze_kroger_products_daily_schedule,
)
from grocery_pipeline.resources.bigquery import bigquery_client


defs = Definitions(
    assets=[
        exploration_kroger_products_daily,
    ],
    jobs=[
        bronze_kroger_products_daily_job,
    ],
    schedules=[
        bronze_kroger_products_daily_schedule,
    ],
    resources={
        "bigquery_client": bigquery_client,
    },
)