#importing Function
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID="windy-orb-439105-b6"
BUCKET='cloudrun_0711'
DATASET_NAME_1='raw_ds'
TABLE_NAME_1='emp_raw'
TABLE_NAME_2='dep_raw'
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_3='empDep_in'
location='us'
SQL_QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
    e.EmployeeID,
    CONCAT(e.FirstName,".",e.LastName) AS FullName,
    e.Email,
    e.Salary,
    e.JoinDate,
    d.DepartmentID,
    d.DepartmentName,
    CAST(e.Salary AS INTEGER) * 0.01 as EmpTax
FROM
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID is not null
"""

#define args
args={"owner":"LAKESH",
"start_date":datetime(2024,11,20),
"retries": 2,
"retry_delay":timedelta(minutes=15)

}

#define dag
with DAG(
    "level_1_DAG",
    description="Moving afile from GCS bucket to bigquery",
    default_args=args,
    schedule_interval= '30 15 * * *',

) as dag:

    task1= GCSToBigQueryOperator(
        task_id="load_employee_data",
        bucket=BUCKET,
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task2 = GCSToBigQueryOperator(
        task_id="load_department_data",
        bucket=BUCKET,
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task3 = BigQueryInsertJobOperator(
        task_id="create_enriched_emp_dep_table",
        configuration={
            "query": {
                "query": SQL_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=location,

    )

(task1,task2)>>task3