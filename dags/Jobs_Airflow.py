import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from Jobs_Extract import job_extraction
from Jobs_Upload import load_jobs
from Jobs_Transform import job_transform


args = {
  'owner': 'airflow',
  'start_date': airflow.utils.dates.days_ago(2),
  'end_date': None,
  'depends_on_past': False,
  'email': ['airflow@example.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

with DAG(
	dag_id = "job_pipeline_demo",
	default_args=args,
	schedule_interval='@daily',
	dagrun_timeout=timedelta(minutes=120),
	description='ETL pipeline for LI jobs',
	start_date = airflow.utils.dates.days_ago(1)
  ) as python_DAG:

    task_extract_jobs = PythonOperator(
        task_id='extract_jobs',
        python_callable=job_extraction
    )

    task_transform_jobs = PythonOperator(
        task_id='transform_jobs',
        python_callable=job_transform
    )

    task_upload_jobs = PythonOperator(
        task_id='upload_jobs',
        python_callable=load_jobs
    )

    task_extract_jobs >> task_transform_jobs >> task_upload_jobs