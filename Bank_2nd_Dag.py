import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
DATASET_NAME_JSON="raw_ds"
TABLE_NAME_JSON='Bank_Loan_raw'

with DAG(
    dag_id="Bank_loan_App_2",
    description="this job is to load data into BQ from GCS",
    start_date=datetime(2024,11,26),
    schedule_interval='30 15 * * *',
    catchup=False

) as dag:
    GCS_to_BQ_job=GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_date_json_async",
        bucket="ingestion_bucket12",
        source_objects=['Bank_application_data_20241127.json/part-00000-f267a39b-8828-4180-9acd-4c5dad7dff4e-c000.json'],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME_JSON}.{TABLE_NAME_JSON}",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        deferrable=True
    )


