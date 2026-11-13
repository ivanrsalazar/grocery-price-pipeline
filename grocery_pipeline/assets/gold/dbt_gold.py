"""Dagster asset that builds and tests the dbt-powered gold layer."""

import os
import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Failure, asset


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DBT_EXECUTABLE = Path(sys.executable).parent / "dbt"


def _run_dbt(context: AssetExecutionContext, args: list[str]) -> None:
    command = [
        str(DBT_EXECUTABLE),
        *args,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--target",
        "local",
    ]
    context.log.info("Running: " + " ".join(command))

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert process.stdout is not None
    for line in process.stdout:
        context.log.info(line.rstrip())

    return_code = process.wait()
    if return_code:
        raise Failure(
            f"dbt {' '.join(args)} failed with exit code {return_code}"
        )


@asset(
    name="gold_dbt_models",
    deps=[AssetKey("silver_exploration_kroger_products")],
    compute_kind="dbt",
)
def gold_dbt_models(context: AssetExecutionContext) -> None:
    """Validate source freshness, then seed, model, and test the gold layer."""
    _run_dbt(context, ["source", "freshness"])
    _run_dbt(context, ["build"])
