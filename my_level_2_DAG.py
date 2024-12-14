#import functions
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator)



#defining variables
PROJECTID="windy-orb-439105-b6"
REGION="us-east1"
CLUSTERNAME="my-cluster-demo"
JOB_FILE_URI="gs://cloudrun_0711/job_dataproc.py"


PYSPARKJOB={
    "reference": {"project_id": PROJECTID},
    "placement": {"cluster_name": CLUSTERNAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
}

#define args:
args={
    "owner":"LAKESH",
    'start_date': datetime(2024,11,21),
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
    }

#define dag:
with DAG(
    dag_id="Level_2_DAG",
    default_args=args,
    schedule_interval="30 15 * * *",
    description="This DAG is to create a Dataproc cluster and submit a job then delete the cluster"
    ) as dag:

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARKJOB,
        region=REGION,
        project_id=PROJECTID
        )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECTID,
        cluster_name=CLUSTERNAME,
        region=REGION,
        )

pyspark_task>>delete_cluster