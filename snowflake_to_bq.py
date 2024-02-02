import csv
from datetime import datetime, timedelta, date
import os

import pandas as pd
import requests
from airflow import DAG, models
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud import storage

'''Email config'''
email_var = Variable.get("email", deserialize_json=True)
team_email: str = email_var["team_dl"]
subject_prefix: str = email_var["subject_prefix"]

'''Project config'''
date = date.today()
projectid = Variable.get("projectid", deserialize_json=True)
gcs_bucket = Variable.get("storage", deserialize_json=True)
project_id: str = projectid["dlfo"]
data_path: str = gcs_bucket["data_path"]


default_args = {
    'start_date': datetime(2020, 9, 30),
    'owner': 'corede',
    'depends_on_past': False,
    'email': ['duy.nguyen@randstadusa.com'], #[team_email], 
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}


def alter_csv_file(snowflake_table, output_file):
    client = storage.Client()
    source_bucket_name = 'cella-snowflake'
    source_blob_name = 'PRD_EDW_DB/ANALYTICS/DIM_BUDGET.csv'

    source_bucket = client.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    csv_data = source_blob.download_as_text()

    rows = csv_data.split('\n')
    rows = [row.replace(r"\\N", r"\N") if r"\\N" in row else row for row in rows]
    altered_csv_data = '\n'.join(rows)

    destination_bucket_name = 'cella-snowflake'
    destination_blob_name = 'Backup_files/altered_DIM_BUDGET.csv'
    destination_bucket = client.get_bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_blob_name)
    destination_blob.upload_from_string(altered_csv_data)



with models.DAG(
    "Load_snowflake_tables_gs_to_bq",
    description='This job loads snowflake data from google storage into BigQuery',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")
    stop_task = DummyOperator(task_id="stop", trigger_rule="none_failed")

    snowflake_table = "gs://cella-snowflake/PRD_EDW_DB/ANALYTICS/DIM_BUDGET.csv"
    output_file = "gs://cella-snowflake/Backup_files/altered_DIM_BUDGET.csv"

    alter_csv_task = PythonOperator(
        task_id='alter_csv_file',
        python_callable=alter_csv_file,
        op_kwargs={'snowflake_table': snowflake_table, 'output_file': output_file}
    )

    load_snowflake_data = BashOperator(
        task_id='load_snowflake_data',
        bash_command=f"bq mk --table --schema PK_BUDGET:INTEGER,WEEK_END_DATE_ID:INTEGER,REVENUE_COST_CENTER:STRING,DEPARTMENT_ID:INTEGER,REVENUE:FLOAT,TEMP_REVENUE:FLOAT,PERM_REVENUE:FLOAT,TEMP_MARGIN:FLOAT,TOTAL_MARGIN:FLOAT,GROSS_PROFIT:FLOAT,COMP:FLOAT,GA:FLOAT {project_id}:Cella_analytics.DIM_BUDGET && bq load --source_format=CSV --skip_leading_rows=1 --replace --allow_jagged_rows=true --null_marker='\\N' --field_delimiter='|' --allow_quoted_newlines {project_id}:Cella_analytics.DIM_BUDGET {output_file}"
    )

    BQ_table_loaded = DummyOperator(task_id="BQ_table_loaded", trigger_rule="none_failed")

    start_task >> alter_csv_task >> load_snowflake_data >> BQ_table_loaded >> stop_task