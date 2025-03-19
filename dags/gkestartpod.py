from airflow import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
from datetime import datetime
from kubernetes.client import models as k8s
from datetime import timedelta

with DAG(
    dag_id='gkestartpod',
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['gke', 'kubernetes', 'gcp'],
    default_args=dict(
        retry_delay=timedelta(seconds=5),
    ),
) as dag:
    run_gke_pod = GKEStartPodOperator(
        task_id='run_gke_pod',
        name='gke-pod-example',
        namespace='default',  # Replace with your namespace
        image='ubuntu:latest',  # Replace with your image
        cmds=['bash', '-cx'],
        arguments=['echo', 'Hello, GKE!'],
        # Specify your GKE cluster details:
        cluster_name='autopilot-cluster',  # Replace with your GKE cluster name
        location='us-central1',  # Replace with your GKE cluster location
        project_id='danny-bq', # Replace with your GCP project ID
        is_delete_operator_pod=True, #delete the pod after completion
        send_logs=True, #send logs to airflow
        # Use a specific GKE service account (optional):
        # google_service_account='your-gke-service-account@your-project.iam.gserviceaccount.com',
    )