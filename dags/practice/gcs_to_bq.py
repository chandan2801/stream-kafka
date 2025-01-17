# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='GCS_to_BQ_and_AGG',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

# Dummy start task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load',
                bucket='project-employee-data',
                source_objects=['employee_data.csv'],
                destination_project_dataset_table='imposing-kite-438605-q1.employee_data.gcs_to_bq_table',
                schema_fields=[
                                {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'job_title', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'department', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'salary', 'type': 'INT64', 'mode': 'NULLABLE'},
                                {'name': 'password', 'type': 'STRING', 'mode': 'NULLABLE'},
                              ],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE', 
    dag=dag)

# BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
    task_id='create_aggr_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql="CREATE OR REPLACE TABLE employee_data.bq_table_aggr AS \
         SELECT \
                job_title,\
                department,\
                SUM(salary) as sum_salary\
         FROM imposing-kite-438605-q1.employee_data.gcs_to_bq_table \
         GROUP BY \
                job_title,\
                department",
    dag=dag)

# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> gcs_to_bq_load >> create_aggr_bq_table >> end