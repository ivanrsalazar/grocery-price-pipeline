from dagster import resource
from google.cloud import bigquery

from grocery_pipeline.settings import get_bq_project


@resource
def bigquery_client(_):
    """
    BigQuery client using Application Default Credentials.
    """
    return bigquery.Client(project=get_bq_project())
