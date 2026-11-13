import unittest
from datetime import datetime, timezone

from dagster import build_schedule_context

from grocery_pipeline.schedules.schedules import (
    bronze_kroger_products_daily_schedule,
)


class KrogerScheduleTests(unittest.TestCase):
    def test_daily_run_key_deduplicates_schedule_tick(self):
        context = build_schedule_context(
            scheduled_execution_time=datetime(2026, 11, 13, 6, tzinfo=timezone.utc)
        )

        execution_data = bronze_kroger_products_daily_schedule.evaluate_tick(context)

        self.assertEqual(len(execution_data.run_requests), 1)
        self.assertEqual(
            execution_data.run_requests[0].run_key,
            "kroger-daily-2026-11-13",
        )


if __name__ == "__main__":
    unittest.main()
