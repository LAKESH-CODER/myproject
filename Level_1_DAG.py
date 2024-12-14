#define imports

import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

#application credentials
GOOGLE_APPLICATION_CREDENTIALS= "C:\\Users\\lakes\\AppData\\Roaming\\gcloud\\credentials.db"

PROJECTID="windy-orb-439105-b6"
Source_bucket="cloudrun_0711"
filename_1="employee.csv"
filename_2="departments.csv"
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
location="US"

INSERT_ROWS_QUERY=f""" 
create or replace table `{PROJECTID}:{DATASET_NAME_2}.{TABLE_NAME_3}` as select 
e.EmployeeID,
e.FirstName,
e.LastName,
e.Email,
e.Salary,
e.JoinDate,
d.DepartmentID,
d.DepartmentName
from 
`{PROJECTID}:{DATASET_NAME_1}.{TABLE_NAME_1}` as e
left join
`{PROJECTID}:{DATASET_NAME_1}.{TABLE_NAME_2}` as d
ON e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID is not null
"""

#defining Defaults Args
args = {
    "owner" : "developer",
    "start_date" : datetime(2024,11,20),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
}


#define the DAG
with DAG(
        dag_id = "Level_1_DAG",
        schedule="52 15 * * *",
        default_args=args,
        description="level-1 DAG for GCS to BQ"
) as dag:
    task_1 = GCSToBigQueryOperator(
        task_id="empfile_gcs_to_bigquery1",
        bucket=Source_bucket,
        source_objects=[filename_1],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    task_2 = GCSToBigQueryOperator(
        task_id="Depfile_gcs_to_bigquery2",
        bucket=Source_bucket,
        source_objects=[filename_2],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_3=BigQueryInsertJobOperator(task_id="insert_query_job",
    configuration={
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location=location,)

#Define DAG tasks


#define task dependency
(task_1,task_2)>>task_3
