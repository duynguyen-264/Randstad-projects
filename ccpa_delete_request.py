import csv
from datetime import datetime, timedelta, date
import os
import re
import imaplib
import email
import logging
from airflow import DAG, models
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from io import StringIO
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

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

def fetch_email():
 

    return email_content


def parsing_email(email_content):
    addresses = []
    lines = email_content.strip().split('\n')
    for line in lines:
        if 'Email Address' in line:  
            email_address = re.search(r'[\w\.-]+@[\w\.-]+\.[\w]+', line)
            if email_address:
                addresses.append(email_address.group())
    return addresses

def body_to_csv(addresses):
    bucket_name = 'cella-snowflake'
    blob_name = 'Backup_files/email.csv'

    headers = ['Email Address']

    csv_string = StringIO()
    writer = csv.writer(csv_string)
    writer.writerow(headers)

    rows = [[address] for address in addresses]
    writer.writerows(rows)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_string.getvalue())

    csv_string.close()



with models.DAG(
    "Automated_CCPA_Delete_Process",
    description='This job automatically process the CCPA delete requests',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")
    stop_task = DummyOperator(task_id="stop", trigger_rule="none_failed")
  

    fetch_email_task = PythonOperator(
        task_id="fetch_email",
        python_callable=fetch_email
    )

    parsing_email_task = PythonOperator(
        task_id="parsing_email",
        python_callable=parsing_email,
        #op_kwargs={'email_content': email_content},
        #op_args=[email_content]
        #op_args=[fetch_email_task.output],
    )

    body_to_csv_task = PythonOperator(
        task_id="body_to_csv",
        python_callable=body_to_csv,
        op_args=[parsing_email_task.output],
    )

    temp_table = "gs://cella-snowflake/Backup_files/email.csv"
    load_csv_to_bq = BashOperator(
        task_id="load_csv_to_bq",
        bash_command=f"bq load --source_format=CSV --skip_leading_rows=1 -- replace --allow_jagged_rows=true {project_id}:CCPA.Temp_table {temp_table}",
    )

    sql_query = """
    INSERT INTO `datalake-frontoffice.CCPA.Delete_Requests`
    SELECT email as email,
        'Delete Requested' as delete_status,
        'No' as rgs_delete,
        'No' as rt_delete,
        'No' as rp_delete,
        'No' as sfo_delete,
        'No' as rls_delete,
        'No' as rhc_delete,
         CURRENT_TIMESTAMP as deleted_date
    FROM (
    SELECT email, ROW_NUMBER() OVER() as row_num
    FROM us-datalakefo-qa-a298.CCPA.Temp_table
    )
    WHERE row_num = (SELECT MAX(row_num) FROM us-datalakefo-qa-a298.CCPA.Temp_table);

    --CALL `datalake-frontoffice.CCPA.Delete_Requests_Process`();
    """

    execute_query_task = BigQueryExecuteQueryOperator(
        task_id='execute_query_task',
        sql=sql_query,
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='id',
        destination_dataset_table='datalake-frontoffice.CCPA.Delete_Requests',  # Modify with the correct table name
    )

start_task >> fetch_email_task >> parsing_email_task >> body_to_csv_task >> load_csv_to_bq >> execute_query_task >> stop_task











import csv
from datetime import datetime, timedelta, date
import os
import re
import imaplib
import email
import logging
from airflow import DAG, models
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from io import StringIO
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery

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

def fetch_email():
    # Set up the BigQuery client
    client = bigquery.Client()

    # Define the query
    query = """
        SELECT emailbody
        FROM `prod-centraldatalake.Activities.GmailScraping_Activities_Dump`
        WHERE emailbody LIKE "%Please delete DSAR approved data regarding the person listed%"
        LIMIT 1
    """

    # Execute the query and fetch the results
    results = client.query(query).to_dataframe()

    if not results.empty:
        email_body = results['emailbody'][0]
        return email_body
    else:
        logging.warning("No matching emails found.")
        return None

def parsing_email(email_content):
    addresses = []
    lines = email_content.strip().split('\n')
    for line in lines:
        if 'Email Address' in line:  
            email_address = re.search(r'[\w\.-]+@[\w\.-]+\.[\w]+', line)
            if email_address:
                addresses.append(email_address.group())
    return addresses

def body_to_csv(addresses):
    bucket_name = 'cella-snowflake'
    blob_name = 'Backup_files/email.csv'

    headers = ['Email Address']

    csv_string = StringIO()
    writer = csv.writer(csv_string)
    writer.writerow(headers)

    rows = [[address] for address in addresses]
    writer.writerows(rows)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_string.getvalue())

    csv_string.close()

with models.DAG(
    "Automated_CCPA_Delete_Process",
    description='This job automatically processes the CCPA delete requests',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")
    stop_task = DummyOperator(task_id="stop", trigger_rule="none_failed")
  
    fetch_email_task = PythonOperator(
        task_id="fetch_email",
        python_callable=fetch_email
    )

    parsing_email_task = PythonOperator(
        task_id="parsing_email",
        python_callable=parsing_email,
        op_args=[fetch_email_task.output],
    )

    body_to_csv_task = PythonOperator(
        task_id="body_to_csv",
        python_callable=body_to_csv,
        op_args=[parsing_email_task.output],
    )

    temp_table = "gs://cella-snowflake/Backup_files/email.csv"
    load_csv_to_bq = BashOperator(
        task_id="load_csv_to_bq",
        bash_command=f"bq load --source_format=CSV --skip_leading_rows=1 --replace --allow_jagged_rows=true {project_id}:CCPA.Temp_table {temp_table}",
    )

    sql_query = """
    INSERT INTO `datalake-frontoffice.CCPA.Delete_Requests`
    SELECT email as email,
        'Delete Requested' as delete_status,
        'No' as rgs_delete,
        'No' as rt_delete,
        'No' as rp_delete,
        'No' as sfo_delete,
        'No' as rls_delete,
        'No' as rhc_delete,
        CURRENT_TIMESTAMP as deleted_date
    FROM (
    SELECT email, ROW_NUMBER() OVER() as row_num
    FROM `{project_id}:CCPA.Temp_table`
    )
    WHERE row_num = (SELECT MAX(row_num) FROM `{project_id}:CCPA.Temp_table`);

    --CALL `datalake-frontoffice.CCPA.Delete_Requests_Process`();
    """

    execute_query_task = BigQueryExecuteQueryOperator(
        task_id='execute_query_task',
        sql=sql_query,
        use_legacy_sql=False,
        location='US',
        gcp_conn_id='id',
        destination_dataset_table='datalake-frontoffice.CCPA.Delete_Requests',  # Modify with the correct table name
    )

start_task >> fetch_email_task >> parsing_email_task >> body_to_csv_task >> load_csv_to_bq >> execute_query_task >> stop_task