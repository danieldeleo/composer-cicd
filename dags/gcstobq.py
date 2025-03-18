"""Example of a Composer DAG that reads a file from GCS and loads it into BigQuery."""
import datetime
from airflow import models  # noqa: F401
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

with models.DAG(
    dag_id="gcstobq",
    schedule_interval=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=True,
    is_paused_upon_creation=False,
    tags=["example"],
) as dag:
    GCSToBigQueryOperator(
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
                "fields": []
            },
            {
                "name": "release_note_type",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": []
            },
            {
                "name": "published_at",
                "mode": "NULLABLE",
                "type": "DATE",
                "description": "",
                "fields": []
            },
            {
                "name": "product_id",
                "mode": "NULLABLE",
                "type": "INTEGER",
                "description": "",
                "fields": []
            },
            {
                "name": "product_name",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": []
            },
            {
                "name": "product_version_name",
                "mode": "NULLABLE",
                "type": "STRING",
                "description": "",
                "fields": []
            }
        ],
        skip_leading_rows=1,
        quote_character='"',
        write_disposition="WRITE_TRUNCATE",
    )