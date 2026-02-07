from pyspark.sql import SparkSession
from tp_utils.common import get_logger, get_dbutils, read_query_from_postgres, write_to_postgres, update_to_postgres,derive_base_path

from datetime import datetime
import pytz
import requests
import os


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    logger = get_logger()
    dbutils = get_dbutils(spark)

    # Retrieve secrets from Databricks
    ref_db_jdbc_url = dbutils.secrets.get('tp_dpf2cdl', 'refDBjdbcURL')
    ref_db_name = dbutils.secrets.get('tp_dpf2cdl', 'refDBname')
    ref_db_user = dbutils.secrets.get('tp_dpf2cdl', 'refDBuser')
    ref_db_pwd = dbutils.secrets.get('tp_dpf2cdl', 'refDBpwd')

    # Define paths for input, working, and rejected files
    base_path = derive_base_path(spark)
    IN_PATH = f"{base_path}/IN/"
    WORK_PATH = f"{base_path}/WORK/"
    REJECT_PATH = f"{base_path}/REJECT/"

    # Retrieve secrets from Databricks
    postgres_schema = dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    ref_db_hostname = dbutils.secrets.get('tp_dpf2cdl', 'refDBhostname')

    # Target table for inserting run metadata
    object_name = f'{postgres_schema}.mm_run_plc'
    rej_obj_name=f'{postgres_schema}.mm_rejct_file_plc'
    obj_wkf_lkp=f'{postgres_schema}.mm_wkf_lkp'

    # Loop through files in the input path
    while dbutils.fs.ls(IN_PATH):

        # List, filter and sort files by modification time (FIFO Logic)
        files = dbutils.fs.ls(IN_PATH)
        files = list(filter(lambda f: f.name.lower().endswith('.zip') and f.size > 0, files))
        sorted_files = sorted(files, key=lambda x: x.modificationTime)
        
        if not sorted_files and not dbutils.fs.ls(IN_PATH):
            logger.info("[INFO] No .zip files found in IN_PATH")
            break


        # Pick the oldest modified file
        fileName = sorted_files[0].name
        fileSize = sorted_files[0].size
        
        fileSize = fileSize / (1024 ** 3)
        logger.info(f'[INFO] fileSize: {fileSize} GB')

        logger.info(f"[INFO] Processing file: {fileName}")

        try:
            # Query to find contract ID and job ID based on file pattern
            query = f"""
                SELECT cntrt_id, wkf_job_id, wkf_ovwrt_ind 
                FROM {postgres_schema}.mm_cntrt_lkp 
                WHERE lower('{fileName}') LIKE lower(file_patrn) 
                and cntrt_sttus_id in (2,3)
            """
            logger.info(f"[DEBUG] Executing pattern match query: {query}")

            df_cntrt_lkp = read_query_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

            # If no match or multiple matches found, reject the file
            if df_cntrt_lkp.count() != 1:
                logger.info(f"[WARNING] File pattern issue for: {fileName}")

                # Prepare data for insertion into mm_rejct_file_plc
                data_rej = [(fileName, REJECT_PATH)]
                columns_rej = ['file_name', 'full_file_path']

                df_rej = spark.createDataFrame(data_rej, columns_rej)

                logger.info(f"[INFO] Inserting reject metadata into: {rej_obj_name}")
                write_to_postgres(df_rej, rej_obj_name, ref_db_jdbc_url, ref_db_name, ref_db_user,ref_db_pwd)
                
                dbutils.fs.mv(f"{IN_PATH}{fileName}", f"{REJECT_PATH}{fileName}")
                logger.info(f"[INFO] Rejected file moved to: {REJECT_PATH}{fileName}")
                continue

            logger.info(f"[INFO] File pattern matched for: {fileName}")
            df_cntrt_lkp.show()

            # Extract contract ID and job ID
            cntrt_id = df_cntrt_lkp.first()['cntrt_id']
            wkf_job_id = df_cntrt_lkp.first()['wkf_job_id']
            wkf_ovwrt_ind = df_cntrt_lkp.first()['wkf_ovwrt_ind']
            logger.info(f"[INFO] Extracted cntrt_id: {cntrt_id}, wkf_job_id: {wkf_job_id}")

            if wkf_ovwrt_ind=='Y':
                if fileSize< 1:
                    cluster_size='Small'
                    logger.info('[DEBUG] Small Cluster')
                elif fileSize < 3:
                    cluster_size='Medium'
                    logger.info('[DEBUG] Medium Cluster')
                else: 
                    cluster_size='Large'
                    logger.info('[DEBUG] Large Cluster')
                
                query=f'''
                    SELECT wkf_job_id 
                    FROM {obj_wkf_lkp}
                    WHERE wkf_name LIKE '%{cluster_size}' 
                    AND wkf_desc = (
                        SELECT wkf_desc 
                        FROM {obj_wkf_lkp}
                        WHERE wkf_job_id = {wkf_job_id}
                    )
                '''
                print(f'[DEBUG] Executing pattern match query:{query}')
                df_wkf_lkp=read_query_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
                wkf_job_id=df_wkf_lkp.first()['wkf_job_id']

            # Prepare data for insertion into mm_run_plc
            cet_tz = pytz.timezone('CET')
            data = [(cntrt_id, 'WAITING', wkf_job_id, fileName, datetime.now(cet_tz), False)]
            columns = ['cntrt_id', 'run_sttus_name', 'wkf_job_id', 'file_name', 'rgstr_file_time', 'user_cancl_or_timedout']

            df = spark.createDataFrame(data, columns)
            logger.info(f"[INFO] Inserting run metadata into: {object_name}")
            df.show()
            write_to_postgres(df, object_name, ref_db_jdbc_url, ref_db_name, ref_db_user,ref_db_pwd)
            
            # Retrieve the run_id of the newly inserted record
            query = f"""
                SELECT * 
                FROM {object_name} 
                WHERE cntrt_id = '{cntrt_id}' AND run_sttus_name = 'WAITING' 
                ORDER BY rgstr_file_time DESC 
                LIMIT 1
            """
            logger.info(f"[DEBUG] Fetching run_id with query: {query}")
            run_id = read_query_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd).first()['run_id']
            

            logger.info(f"[INFO] run_id generated is: {run_id}")

            # Construct new file name with run_id and timestamp
            file_name,file_extn=os.path.splitext(fileName)
            date_time = datetime.now(cet_tz).strftime("%Y%m%d_%H%M%S")
            dest_fileName = f"{file_name}_{run_id}_{date_time}{file_extn}"
            logger.info(f"[INFO] Renaming file to: {dest_fileName}")

            
            # Update the file name in the database            
            query = f"""
                UPDATE {postgres_schema}.mm_run_plc 
                SET file_name = %s
                WHERE run_id = %s
            """
            params=(dest_fileName,run_id)

            logger.info(f"[DEBUG] Updating file name in DB: {query}")
            update_to_postgres(query,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)

            # Move the file to the working directory with the new name
            dbutils.fs.mv(f"{IN_PATH}{fileName}", f"{WORK_PATH}{dest_fileName}")    
            logger.info(f"[INFO] Moved file to working path: {WORK_PATH}{dest_fileName}")
            logger.info('------------------------------ END OF ITERATION ----------------------------------')
        

        except Exception as e:
            logger.info(f"[ERROR] Exception occurred while processing file {fileName}: {e}")
            break
    
    logger.info("[INFO] No more files in the IN directory")
