from dagster import DefaultScheduleStatus, RunRequest, schedule

from grocery_pipeline.jobs.jobs import (
    bronze_kroger_products_daily_job,
)


@schedule(
    name="bronze_kroger_products_daily_schedule",
    job=bronze_kroger_products_daily_job,
    cron_schedule="0 6 * * *",  # daily at 06:00 UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
def bronze_kroger_products_daily_schedule(context):
    """Use a stable daily run key so multiple daemons cannot launch duplicates."""
    scheduled_at = context.scheduled_execution_time
    run_date = scheduled_at.date().isoformat()
    return RunRequest(
        run_key=f"kroger-daily-{run_date}",
        tags={"pipeline": "kroger_daily", "snapshot_date": run_date},
    )
