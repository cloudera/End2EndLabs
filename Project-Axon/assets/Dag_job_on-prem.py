from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

def _check_table_exists(ti):
    """
    Checks if the summary table exists. 
    Returns the task_id of the next step to follow.
    """
    results = ti.xcom_pull(task_ids="check_table_metadata")
    # If 'show tables' returns the table name, it exists
    if results and len(results) > 0:
        return "fetch_missing_date"
    return "fetch_initial_date"

def _extract_date_and_check(ti):
    """
    Pulls results from whichever branch was executed.
    """
    # Pull from both potential upstream tasks
    missing_date_res = ti.xcom_pull(task_ids="fetch_missing_date")
    initial_date_res = ti.xcom_pull(task_ids="fetch_initial_date")
    
    # Use whichever one has data
    results = missing_date_res or initial_date_res

    if results and results[0][0]:
        date_str = str(results[0][0])
        print(f"Found date to process: {date_str}")
        return date_str
    
    print("No dates found. Short-circuiting.")
    return None

with DAG(
    dag_id="callcenter_sync_prod_v1",
    default_args=default_args,
    schedule_interval="30 16 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. Check if the table exists in the metadata
    check_table_metadata = SQLExecuteQueryOperator(
        task_id="check_table_metadata",
        conn_id="impala_default",
        sql="SHOW TABLES IN callcenter_data LIKE 'callcenter_interaction_summary'",
    )

    # 2. Decide which SQL path to take
    branch_step = BranchPythonOperator(
        task_id="check_first_run_branch",
        python_callable=_check_table_exists,
    )

    # 3. Path A: Table exists - find the gap
    fetch_missing_date = SQLExecuteQueryOperator(
        task_id="fetch_missing_date",
        conn_id="impala_default",
        sql="""
            SELECT MIN(interactiondate) 
            FROM callcenter_data.callcenter_interaction 
            WHERE interactiondate NOT IN (SELECT DISTINCT interactiondate FROM callcenter_data.callcenter_interaction_summary)
        """,
    )

    # 4. Path B: Table missing - get the very first record date
    fetch_initial_date = SQLExecuteQueryOperator(
        task_id="fetch_initial_date",
        conn_id="impala_default",
        sql="SELECT MIN(interactiondate) FROM callcenter_data.callcenter_interaction",
    )

    # 5. Extract date and stop if empty
    # trigger_rule is key here: it allows the DAG to continue even though one branch was skipped
    check_date = ShortCircuitOperator(
        task_id="check_date_task",
        python_callable=_extract_date_and_check,
        trigger_rule="none_failed_min_one_success"
    )

    # 6. Trigger Spark job
    run_spark = CDEJobRunOperator(
        task_id="run_spark_job",
        job_name="CallCenterSummary",
        variables={
            "interaction_date": "{{ ti.xcom_pull(task_ids='check_date_task') }}"
        }
    )

    # Dependency Graph
    check_table_metadata >> branch_step
    branch_step >> [fetch_missing_date, fetch_initial_date]
    fetch_missing_date >> check_date
    fetch_initial_date >> check_date
    check_date >> run_spark