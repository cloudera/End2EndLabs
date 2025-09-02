import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# Variable Values
S3_BUCKET = "project-axon-buk-364703ca"
DATES_FILE = "my-data/InteractionDate/dates_to_run.txt"
PROCESSED_FILE = "my-data/InteractionDate/processed_dates.txt"
SPARK_JOB_NAME = "CallCenterSummary"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _find_next_date_and_bucket(ti, dates_file_key, processed_file_key):
    """
    This function is executed by a PythonOperator at runtime.
    It finds the next unprocessed date and pushes it to XCom along with the bucket name.
    """
    try:
        bucket_name = S3_BUCKET
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        all_dates_raw = s3_hook.read_key(key=dates_file_key, bucket_name=bucket_name)
        all_dates = set(all_dates_raw.strip().split('\n'))

        try:
            processed_dates_raw = s3_hook.read_key(key=processed_file_key, bucket_name=bucket_name)
            processed_dates = set(processed_dates_raw.strip().split('\n'))
        except Exception:
            print(f"Warning: {processed_file_key} not found on S3. Assuming no dates processed yet.")
            processed_dates = set()
        
        unprocessed_dates = sorted(list(all_dates - processed_dates))
        
        if not unprocessed_dates:
            print("No unprocessed dates found. The subsequent tasks will be skipped.")
            ti.xcom_push(key='next_date_to_process', value=None)
            ti.xcom_push(key='s3_bucket', value=bucket_name)
            return None
        
        next_date = unprocessed_dates[0]
        print(f"Found new date to process: {next_date}")
        ti.xcom_push(key='next_date_to_process', value=next_date)
        ti.xcom_push(key='s3_bucket', value=bucket_name)
        return next_date

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def _update_processed_file(ti, processed_file_key):
    """
    Python function to append the processed date to the processed.txt file on S3.
    """
    try:
        next_date = ti.xcom_pull(task_ids='find_next_date_task', key='next_date_to_process')
        if not next_date:
            print("No date was processed, skipping file update.")
            return

        s3_bucket = ti.xcom_pull(task_ids='find_next_date_task', key='s3_bucket')
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        try:
            existing_content = s3_hook.read_key(key=processed_file_key, bucket_name=s3_bucket)
            new_content = existing_content.strip() + '\n' + next_date
        except Exception:
            new_content = next_date
        
        s3_hook.load_string(string_data=new_content, key=processed_file_key, bucket_name=s3_bucket, replace=True)
        print(f"Successfully updated {processed_file_key} with date {next_date}")

    except Exception as e:
        print(f"An error occurred while updating the processed file: {e}")
        raise

with DAG(
    dag_id="process_interaction_dates",
    default_args=default_args,
    schedule_interval="30 16 * * *",
    catchup=False
) as dag:
    
    find_next_date_task = PythonOperator(
        task_id="find_next_date_task",
        python_callable=_find_next_date_and_bucket,
        op_kwargs={
            'dates_file_key': 'DATES_FILE',
            'processed_file_key': PROCESSED_FILE,
        }
    )

    run_cde_spark = CDEJobRunOperator(
        task_id="run_cde_spark_job",
        job_name=SPARK_JOB_NAME,
        # CORRECTED: Use 'variables' to pass a dictionary of named arguments.
        variables={
            'interaction_date': '{{ ti.xcom_pull(task_ids="find_next_date_task", key="next_date_to_process") }}'
        },
        trigger_rule='one_success'
    )
    
    update_processed = PythonOperator(
        task_id="update_processed_dates",
        python_callable=_update_processed_file,
        op_kwargs={
            'processed_file_key': PROCESSED_FILE,
        }
    )

    find_next_date_task >> run_cde_spark >> update_processed