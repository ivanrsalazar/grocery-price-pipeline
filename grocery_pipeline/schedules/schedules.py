from dagster import ScheduleDefinition

from grocery_pipeline.jobs.jobs import bronze_kroger_products_daily_job


bronze_kroger_products_daily_schedule = ScheduleDefinition(
    name="bronze_kroger_products_daily_schedule",
    job=bronze_kroger_products_daily_job,
    cron_schedule="0 6 * * *",  # daily at 06:00 UTC
    execution_timezone="UTC",
)