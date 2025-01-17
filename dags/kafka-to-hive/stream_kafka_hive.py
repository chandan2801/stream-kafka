# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 11:48:02 2025

@author: chandan.pr
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Replace with your configurations
PROJECT_ID = "imposing-kite-438605-q1"
REGION = "us-central1"
CLUSTER_NAME = "shared-cluster-1"
BUCKET_NAME = "imposing-kite-438605-q1"
SPARK_JOB_JAR = "gs://imposing-kite-438605-q1/etl-scalaPractice-stage.jar"
CLASS_NAME = "main.WikiKafkaStream"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="stream_kafka_hive",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dataproc", "spark"],
) as dag:

    # Submit Spark job to Dataproc
    spark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "main_class": CLASS_NAME,
            "jar_file_uris": [SPARK_JOB_JAR],
        },
    }

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=spark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
    
    submit_spark_job