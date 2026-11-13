"""Runtime settings shared by the Kroger pipeline."""

import os


DEFAULT_BQ_PROJECT = "grocery-pipe-line"


def get_bq_project() -> str:
    """Return the target BigQuery project, allowing local overrides."""
    return os.getenv("GROCERY_BQ_PROJECT", DEFAULT_BQ_PROJECT)
