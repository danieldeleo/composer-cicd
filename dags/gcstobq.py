"""Example of a Composer DAG that reads a file from GCS and loads it into BigQuery."""

import datetime

from airflow import models  # noqa: F401
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

with models.DAG(
    dag_id="gcstobq",
    schedule_interval=None,
    start_date=datetime.datetime(2021, 1, 1),
    is_paused_upon_creation=False,
    tags=["example"],
) as dag:
    # BigQueryToGCS operator which exports a BigQuery table to GCS
    export_table = BigQueryToGCSOperator(
        task_id="export_table",
        source_project_dataset_table="bigquery-public-data:google_cloud_release_notes.release_notes",
        destination_cloud_storage_uris=["gs://vz-lineage-demo/release-notes.csv"],
        export_format="CSV",
        field_delimiter=",",
        print_header=True,
        deferrable=True,
    )

    load_table = GCSToBigQueryOperator(
        task_id="task",
        bucket="vz-lineage-demo",
        source_objects=["release-notes.csv"],
        destination_project_dataset_table="vz-assessment:testing.release_notes",
        schema_fields=[
            {
                "name": "description",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": [],
            },
            {
                "name": "release_note_type",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": [],
            },
            {
                "name": "published_at",
                "mode": "NULLABLE",
                "type": "DATE",
                "description": "",
                "fields": [],
            },
            {
                "name": "product_id",
                "mode": "NULLABLE",
                "type": "BIGNUMERIC",
                "description": "",
                "fields": [],
            },
            {
                "name": "product_name",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": [],
            },
            {
                "name": "product_version_name",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": [],
            },
        ],
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        deferrable=True,
    )
    # BigQueryInsertJobOperator which executes a SQL query to SELECT from the realease_notes table
    # and inserts the results into the release_notes_copy table
    elt_sql = BigQueryInsertJobOperator(
        task_id="bq_insert",
        configuration={
            "query": {
                "query": "SELECT * FROM `vz-assessment.testing.release_notes` ORDER BY published_at DESC LIMIT 10",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "vz-assessment",
                    "datasetId": "testing",
                    "tableId": "10_latest_release_notes",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        deferrable=True,
    )
    export_table >> load_table >> elt_sql
