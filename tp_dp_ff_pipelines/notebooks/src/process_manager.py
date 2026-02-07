from pyspark.sql import SparkSession
from tp_utils.common import get_logger, get_dbutils, read_query_from_postgres, write_to_postgres, update_to_postgres, read_from_postgres, get_database_config

import time
from functools import wraps
import requests
from pyspark.sql.types import StructType, StructField, LongType


# -------------------------------
# Retry Decorator with Exponential Backoff
# -------------------------------
def retry_on_exception(max_retries=3, delay=5, backoff=2, exceptions=(requests.exceptions.RequestException,)):
    
    """
    A decorator that retries a function call on specified exceptions using exponential backoff.

    Args:
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 3.
        delay (int, optional): Initial delay in seconds before the first retry. Defaults to 5.
        backoff (int, optional): Multiplier for exponential backoff. Each retry delay is multiplied by this factor. Defaults to 2.
        exceptions (tuple, optional): Tuple of exception types to catch and retry on. Defaults to (requests.exceptions.RequestException,).

    Returns:
        function: A wrapped function that includes retry logic. If the function fails with one of the specified exceptions,
                  it will be retried up to `max_retries` times with increasing delay. If all retries fail, the exception is raised.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    logger.info(f"[WARN] Attempt {retries} failed with error: {e}")
                    if retries == max_retries:
                        logger.info(f"[ERROR] Max retries reached. Giving up.")
                        raise
                    logger.info(f"[INFO] Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


# -------------------------------
# API Call: Trigger New Job
# -------------------------------
@retry_on_exception()
def trigger_new_job(run_id, cntrt_id, job_id, file_name):
    
    """
    Triggers a new Databricks job using the REST API.

    Args:
        run_id (int): Unique identifier for the run.
        cntrt_id (int): Contract ID associated with the job.
        job_id (int): Databricks job ID to trigger.
        file_name (str): Name of the file to process.

    Returns:
        dict: Dictionary containing success status and either the response data or error message.
    """

    url = f"{databricks_instance}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": job_id,
        "job_parameters": {
            "RUN_ID": run_id,
            "CNTRT_ID": cntrt_id,
            "FILE_NAME": file_name
        }
    }
    print(f"[INFO] Triggering new job with payload: {payload}")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    print(f"[SUCCESS] Job triggered successfully: {response.json()}")
    return {"success": True, "data": response.json()}

# -------------------------------
# API Call: Fetch Latest Repair ID
# -------------------------------
@retry_on_exception()
def fetch_latest_rpr_id(wkf_job_run_id):
    """
    Fetches the latest repair ID for a given Databricks job run using the REST API.

    Args:
        wkf_job_run_id (int): The run ID of the Databricks job workflow.

    Returns:
        dict: Dictionary containing success status and either the response data or error message.
    """

    url = f"{databricks_instance}/api/2.2/jobs/runs/get"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "run_id": wkf_job_run_id,
        "include_history": True
    }
    response = requests.get(url, headers=headers, json=payload)
    response.raise_for_status()
    print(f"[SUCCESS] Fetched the latest repair id successfully")
    return {"success": True, "data": response.json()}

# -------------------------------
# API Call: Repair Job Run
# -------------------------------
@retry_on_exception()
def repair_job_run(run_id, cntrt_id, wkf_job_run_id, wkf_job_rpr_id, file_name):

    """
    Repairs a failed Databricks job run using the REST API.

    Args:
        run_id (int): Unique identifier for the run.
        cntrt_id (int): Contract ID associated with the job.
        wkf_job_run_id (int): The run ID of the Databricks job workflow.
        wkf_job_rpr_id (int or None): The latest repair ID, if available.
        file_name (str): Name of the file to process.

    Returns:
        dict: Dictionary containing success status and either the response data or error message.
    """

    logger = get_logger()
    url = f"{databricks_instance}/api/2.2/jobs/runs/repair"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "run_id": wkf_job_run_id,
        "rerun_all_failed_tasks": True,
        "job_parameters": {
            "RUN_ID": run_id,
            "CNTRT_ID": cntrt_id,
            "FILE_NAME": file_name
        }
    }
    if wkf_job_rpr_id is not None:
        print("[INFO] Previous repair ID found, including in payload.")
        payload["latest_repair_id"] = wkf_job_rpr_id

    print(f"[DEBUG] Repair job payload: {payload}")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"[SUCCESS] Job repair triggered successfully: {response.json()}")
        return {"success": True, "data": response.json()}
    
    except Exception as e:
        print(f"Unable to Repair the job. We have an exception: {e}")
        return {"success": False, "data": response.json()}


# -------------------------------
# Main Execution Block
# -------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    logger = get_logger()
    dbutils = get_dbutils(spark)


    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url = db_config['ref_db_jdbc_url']
    ref_db_name = db_config['ref_db_name']
    ref_db_user = db_config['ref_db_user']
    ref_db_pwd = db_config['ref_db_pwd']
    postgres_schema = db_config['postgres_schema']
    ref_db_hostname = db_config['ref_db_hostname']

    # Securely fetch the Databricks access token from the secret scope
    databricks_token = dbutils.secrets.get('tp_dpf2cdl', 'databricks-access-token')

    # Databricks workspace URL
    databricks_instance = dbutils.secrets.get('tp_dpf2cdl', 'databricks-instance')


    
    # Main processing loop
    wait_vw = f'{postgres_schema}.mm_run_wait_vw'

    while True:
        print('--------------------------------------------------------------------------------')
        logger.info(f"[INFO] Reading data from Postgres view: {wait_vw}")
        df_waiting_vw = read_from_postgres(wait_vw, spark, ref_db_jdbc_url, ref_db_name,ref_db_user,ref_db_pwd)
        logger.info(f"[INFO] Total number of records in waiting {df_waiting_vw.count()}")
        # Exit loop if no records are found
        if not df_waiting_vw.count():
            logger.info("[INFO] No records found. Exiting loop.")
            break

        arr_waiting = df_waiting_vw.collect()

        for record in arr_waiting:
            run_id = record.run_id
            cntrt_id = record.cntrt_id
            file_name = record.file_name
            wkf_job_id = record.wkf_job_id
            wkf_job_run_id = record.wkf_job_run_id

            logger.info(f"[INFO] Processing record: run_id={run_id}, cntrt_id={cntrt_id}, file_name={file_name}")

            if wkf_job_run_id is None:
                # No previous run, trigger a new job
                logger.info(f"[INFO] No previous run found. Triggering new job for run_id: {run_id}")
                result = trigger_new_job(run_id, cntrt_id, wkf_job_id, file_name)

                if not result.get("success"):
                    logger.info(f"[FATAL] Job trigger failed. Terminating workflow for run_id: {run_id}")
                    raise Exception(f"Databricks job trigger failed")

                query = f"UPDATE {postgres_schema}.mm_run_plc SET run_sttus_name='SCHEDULED' WHERE run_id={run_id}"
                logger.info(f"[INFO] Updating status in Postgres: {query}")
                params=()
                update_to_postgres(query,params, ref_db_name,ref_db_user,ref_db_pwd,ref_db_hostname)
            else:
                # Previous run exists, attempt to repair
                logger.info(f"[INFO] Previous run found. Attempting repair for run_id: {run_id}")

                response_data_rpr_id=fetch_latest_rpr_id(wkf_job_run_id)
                repair_history=response_data_rpr_id["data"]['repair_history']
                wkf_job_rpr_id = sorted(repair_history, key=lambda x: x['start_time'], reverse=True)[0].get('id')

                if wkf_job_rpr_id is None:
                     logger.info(" No previous repair found")
                else:
                    logger.info(f"[DEBUG] Latest wkf_job_rpr_id is {wkf_job_rpr_id} ")

                response_data = repair_job_run(run_id, cntrt_id, wkf_job_run_id, wkf_job_rpr_id, file_name)

                if not response_data.get("success"):
                    logger.info(f"[FATAL] Job repair failed. Terminating workflow for run_id: {run_id}")
                    query = f"UPDATE {postgres_schema}.mm_run_plc SET run_sttus_name='FAILED' WHERE run_id={run_id}"
                    params=()
                    update_to_postgres(query,params, ref_db_name,ref_db_user,ref_db_pwd,ref_db_hostname)
                    continue
                else:
                    logger.info("[INFO] Repair successful. Updating status in Postgres.")
                    query = f"UPDATE {postgres_schema}.mm_run_plc SET run_sttus_name='SCHEDULED' WHERE run_id={run_id}"
                    params=()
                    update_to_postgres(query,params, ref_db_name,ref_db_user,ref_db_pwd,ref_db_hostname)

                    repair_id = response_data["data"].get("repair_id")
                    logger.info(f"[INFO] New repair ID received: {repair_id}")
