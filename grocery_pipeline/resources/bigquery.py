from dagster import resource
from google.cloud import bigquery


@resource
def bigquery_client(_):
    """
    BigQuery client using ambient GCP credentials
    (VM service account).
    """
    return bigquery.Client(project="grocery-pipe-line")