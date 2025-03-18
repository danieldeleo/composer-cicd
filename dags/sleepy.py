"""Example of a Composer DAG that runs a long-running (5min) KubernetesPodOperator with retries."""

import datetime

import airflow
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with models.DAG(
    dag_id="sleepy",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=100,
    default_args={
        "retries": 10,
        "retry_delay": datetime.timedelta(seconds=10),
    },
) as dag:
    for x in range(10):
        task_id = f"sleep{x}"
        tpt = KubernetesPodOperator(
            task_id=task_id,
            name="sleepy",
            cmds=["bash"],
            arguments=[
                "-c",
                rf"""
                set -e && \
                echo "Try number: $AIRFLOW_RETRY_NUMBER" && \
                echo "Sleeping for 5 minutes" && \
                sleep 5m
                """,
            ],
            env_vars={"AIRFLOW_RETRY_NUMBER": "{{ task_instance.try_number }}"},
            namespace="composer-user-workloads",
            image="teradata/tpt:latest",
            config_file="/home/airflow/composer_kube_config",
            kubernetes_conn_id="kubernetes_default",
            container_resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": "100m",
                    "memory": "64Mi",
                },
                limits={
                    "cpu": "100m",
                    "memory": "64Mi",
                },
            ),
            # Increase pod startup timeout to 10 minutes
            startup_timeout_seconds=600,
        )
