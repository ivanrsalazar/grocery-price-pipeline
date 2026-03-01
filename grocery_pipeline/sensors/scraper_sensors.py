"""
Per-retailer sensors that emit RunRequests based on retailer_zipcodes.yaml.

Each sensor fires at most once per retailer+zipcode+day (via run_key dedup).
"""

from datetime import datetime, timezone
from pathlib import Path

import yaml
from dagster import RunRequest, sensor, SkipReason

from grocery_pipeline.assets.bronze.bronze_scraper_products import RETAILER_REGISTRY
from grocery_pipeline.jobs.jobs import per_retailer_jobs

_ZIPCODES_PATH = (
    Path(__file__).parent.parent / "config" / "retailer_zipcodes.yaml"
)


def _load_zipcodes() -> dict[str, list[str]]:
    data = yaml.safe_load(_ZIPCODES_PATH.read_text(encoding="utf-8"))
    return data.get("retailers", {})


def build_scraper_sensors():
    sensors = []

    for retailer, job in per_retailer_jobs.items():
        info = RETAILER_REGISTRY[retailer]
        asset_name = f"bronze_{retailer}_products_daily"

        def _make_sensor(retailer=retailer, job=job, info=info, asset_name=asset_name):
            @sensor(
                name=f"sensor_bronze_{retailer}_products",
                job=job,
                minimum_interval_seconds=3600,
            )
            def _sensor_fn(context):
                zipcodes_by_retailer = _load_zipcodes()
                zipcodes = zipcodes_by_retailer.get(retailer, [])

                if not zipcodes:
                    yield SkipReason(f"No zipcodes configured for {retailer}")
                    return

                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                for zipcode in zipcodes:
                    run_key = f"{retailer}_{zipcode}_{today}"

                    # Build CDP URL from registry defaults
                    cdp_url = ""
                    if info["connection"] == "cdp" and info.get("default_cdp_port"):
                        cdp_url = f"http://127.0.0.1:{info['default_cdp_port']}"

                    yield RunRequest(
                        run_key=run_key,
                        run_config={
                            "ops": {
                                asset_name: {
                                    "config": {
                                        "zipcode": zipcode,
                                        "cdp_url": cdp_url,
                                    }
                                }
                            }
                        },
                        tags={
                            "pipeline_lock": "ec2_exclusive",
                            "retailer": retailer,
                            "zipcode": zipcode,
                        },
                    )

            return _sensor_fn

        sensors.append(_make_sensor())

    return sensors


scraper_sensors = build_scraper_sensors()
