from pyspark.sql import SparkSession
from tp_utils.common import get_logger, get_dbutils, read_query_from_postgres, update_to_postgres, get_database_config, read_run_params
import requests

# Fetch new_wkf_job_id from Databricks
def fetch_databricks_jobs(api_url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{api_url}/api/2.1/jobs/list", headers=headers)
    jobs = response.json().get("jobs", [])
    return {job["settings"]["name"]: job["job_id"] for job in jobs}



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
    ref_db_hostname = db_config['ref_db_hostname']
    postgres_schema = db_config['postgres_schema']
    
    # Get the job parameters
    args = read_run_params()
    job_name=args.databricks_wkf_name
    wkf_name=args.postgres_wkf_name
    print(job_name)
    print(wkf_name)

    # Construct the dictionary
    job_mapping = [{"job_name": job_name, "wkf_name": wkf_name}]
    logger.info(f"{job_mapping}")

    df_old_ids = read_query_from_postgres(f"SELECT wkf_name, wkf_job_id, wkf_desc, oper_cnt FROM {postgres_schema}.mm_wkf_lkp",spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    display(df_old_ids)

    # Securely fetch the Databricks access token from the secret scope
    databricks_token = dbutils.secrets.get('tp_dpf2cdl', 'databricks-access-token')

    # Databricks workspace URL
    databricks_instance = dbutils.secrets.get('tp_dpf2cdl', 'databricks-instance')

    new_job_ids = fetch_databricks_jobs(databricks_instance, databricks_token)
   

    # Build final list
    final_mapping = []
    for entry in job_mapping:
        job_name = entry["job_name"]
        wkf_name = entry["wkf_name"]
        new_wkf_job_id = new_job_ids.get(job_name)
        
        old_row = df_old_ids[df_old_ids["wkf_name"] == wkf_name]
        old_wkf_job_id = int(old_row.collect()[0].wkf_job_id)
        wkf_desc=str(old_row.collect()[0].wkf_desc)
        wkf_oper_cnt=int(old_row.collect()[0].oper_cnt)

        final_mapping.append({
            "job_name": job_name,
            "new_wkf_job_id": new_wkf_job_id,
            "wkf_name": wkf_name,
            "old_wkf_job_id": old_wkf_job_id,
            "wkf_desc": wkf_desc,
            "oper_cnt": wkf_oper_cnt
        })

    query_insert_wkf_lkp=''
    query_update_cntrt_lkp=''
    query_update_run_plc=''
    query_update_wkf_task_assoc=''
    query_del_wkf_lkp=''
    query_update_wkf_lkp=''

    for item in final_mapping:
        
        query_insert_wkf_lkp+=f"INSERT INTO {postgres_schema}.mm_wkf_lkp VALUES({item['new_wkf_job_id']},'{item['wkf_name']}_Test', '{item['wkf_desc']}',{item['oper_cnt']} ); "

        query_update_cntrt_lkp+=f"UPDATE {postgres_schema}.mm_cntrt_lkp SET wkf_job_id={item['new_wkf_job_id']} WHERE wkf_job_id= {item['old_wkf_job_id']}; "

        query_update_run_plc+=f"UPDATE {postgres_schema}.mm_run_plc SET wkf_job_id={item['new_wkf_job_id']} WHERE wkf_job_id= {item['old_wkf_job_id']}; "

        query_update_wkf_task_assoc+=f"UPDATE {postgres_schema}.mm_wkf_task_assoc SET wkf_job_id={item['new_wkf_job_id']} WHERE wkf_job_id= {item['old_wkf_job_id']}; "

        query_del_wkf_lkp+=f"DELETE FROM {postgres_schema}.mm_wkf_lkp WHERE wkf_job_id= {item['old_wkf_job_id']}; "
        
        query_update_wkf_lkp+=f"UPDATE {postgres_schema}.mm_wkf_lkp SET wkf_name='{item['wkf_name']}' WHERE wkf_job_id={item['new_wkf_job_id']}; "
    params=()
    logger.info(f"{query_insert_wkf_lkp}")
    update_to_postgres(query_insert_wkf_lkp,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
    print('---------------------------------------------------------------------------------')
    logger.info(f"{query_update_cntrt_lkp}")
    update_to_postgres(query_update_cntrt_lkp,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
    print('---------------------------------------------------------------------------------')
    logger.info(f"{query_update_run_plc}")
    update_to_postgres(query_update_run_plc,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
    print('---------------------------------------------------------------------------------')
    logger.info(f"{query_update_wkf_task_assoc}")
    update_to_postgres(query_update_wkf_task_assoc,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
    print('---------------------------------------------------------------------------------')
    logger.info(f"{query_del_wkf_lkp}")
    update_to_postgres(query_del_wkf_lkp, params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
    print('---------------------------------------------------------------------------------')
    logger.info(f"{query_update_wkf_lkp}")
    update_to_postgres(query_update_wkf_lkp, params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
