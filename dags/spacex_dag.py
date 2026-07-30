
from airflow.sdk import dag, task, task_group, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.edgemodifier import Label
from pathlib import Path
from datetime import datetime, timedelta
import requests
import json
import os
import logging
import ast
from common.utils import *

logger = logging.getLogger(__name__)



def metadata_payload(source_system:str, endpoint:str, logical_date:str, 
                     response:requests.Response, file_path:str, record_count:int):
    return {
        "source_system": source_system,
        "endpoint": endpoint,
        "execution_date": logical_date,
        "http_status": response.status_code,
        "content_length": response.headers.get("Content-Length"),
        "content_type": response.headers.get("Content-Type"),
        "file_name": file_path.split("/")[-1],
        "file_path": file_path,
        "record_count": record_count,
    }

def save_file(data, file_path):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Data saved to {file_path}")
    except Exception as e:
        logger.error(f"Error occurred while saving data to file: {e}")
        raise


@task
def ingestion_api_task(entity_name: str, api_url: str, source_bucket: str, data_source: str, file_name: str, destination_key_pattern: str, **kwargs):
    logical_date = kwargs["logical_date"].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    current_dt = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
    
    try:

        year = current_dt.year
        month = current_dt.month
        day = current_dt.day
        key_path = destination_key_pattern.replace("\g<year>", str(year)).replace("\g<month>", str(month)).replace("\g<day>", str(day)).replace("\g<filename>", file_name)
        directory = "/".join(key_path.split("/")[:-1])
        mt_file_path = f"{directory}/{data_source}_api_metadata.json"
        metadata_list = []
        
        if (not isinstance(api_url, str)) and isinstance(api_url, list):
            api_param = 'SPACEX_API_URL'
            api_url = ast.literal_eval(Variable.get(api_param))
            data_list = []
            for i, url in enumerate(api_url):
                
                logger.info("Fetching %s", url)
                
                response = requests.get(url, timeout= 30)
                response.raise_for_status()
                data = response.json()
                
                record_count = len(data) if isinstance(data, list) else 1
                metadata = metadata_payload("SpaceX", url, logical_date, response, key_path, record_count)
                
                metadata_list.append(metadata)
                data_list.append(data)
                
                logger.info(f'Save source file to path: {key_path}')
                logger.info(data)
                
                save_file(data, key_path)
                logger.info("Finished %s", url)
                
            logger.info(f"Saving metadata to {mt_file_path}")
            save_file(metadata_list, mt_file_path)
            return data_list
        
        else:
            response = requests.get(api_url, timeout= 300)
            response.raise_for_status()

            data = response.json()
            
            metadata = metadata_payload(
                source_system="SpaceX",
                endpoint=api_url,
                logical_date=logical_date,
                response=response,
                file_path=key_path,
                record_count=len(data) if isinstance(data, list) else 1,
            )

            save_file(data, key_path)
            save_file([metadata], mt_file_path)
            return [data]
        
    except Exception as e:
        logger.error(f"Error occurred while fetching data: {e}")
        raise

@task.branch
def precheck_file_exist(upstream_task_id:str, entity_name, **kwargs):
    ti = kwargs['ti']
    ingested_data = ti.xcom_pull(task_ids=upstream_task_id)
    if ingested_data:
        return f'file_exist-{entity_name}'
    else:
        return f'file_not_exist-{entity_name}'


        
@dag(
    dag_id = "spacex_job",
    start_date=datetime(2026, 7, 20),
    schedule="@daily",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    catchup=True,
    tags=['SATELLITES']
)


def main():
    
    data_source = "spacex"
    config_path = Variable.get('CONFIG_PATH')
    env = Variable.get("ENVIRONMENT")
    docker_dbt_image = Variable.get('docker_dbt_image')

    
    # ----------- empty operator -------------
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    # ----------------------------------------
    
    header_keys, dict_profiles = get_datasource_profile(config_path, env, data_source)
    
    for profile in dict_profiles:
        print("profile: ", profile)
        entity_name = profile.get("entityName")
        file_name = profile.get("fileNamePattern")
        api_url = profile.get("api_url")
        source_bucket = profile.get("masterBucket")
        destination_key_pattern = profile.get("destinationKeyPattern")
        dbt_load = profile.get('dbtLoad', None)
        
        ingestion_task = ingestion_api_task.override(task_id=f'ingest-{entity_name}')\
                            (entity_name, api_url, source_bucket, data_source, file_name,  destination_key_pattern)
        
        precheck_file = precheck_file_exist.override(task_id=f'check_file_exist-{entity_name}') \
                        (upstream_task_id = f'ingest-{entity_name}', entity_name = entity_name)
                        
        file_exist = EmptyOperator(task_id=f'file_exist-{entity_name}')
        file_not_exist = EmptyOperator(task_id=f'file_not_exist-{entity_name}')
        
        
        
        
        if dbt_load:
            @task_group(group_id=f'load-{entity_name}', prefix_group_id=False)
            def dbt_load_entity_level(dbt_load, entity_name, **kwargs):
                dbt_tasks = []
                previous_task = None
                
                for dbt_item in dbt_load:
                    dbt_task_name = dbt_item['name']
                    cmd_args = dbt_item["args"] + ["--vars", '{"intakeDate": "{{ ds }}"}']
                    current_task_id = f"dbt-{dbt_task_name}-{entity_name}"
                    
                    current_task = KubernetesPodOperator(
                        task_id=current_task_id,
                        name=current_task_id,
                        image=docker_dbt_image,
                        
                        # Injects the clean array arguments
                        arguments=cmd_args, 
                        
                        labels={"component": "dbtrunner"},
                        namespace=os.getenv("AIRFLOW__KUBERNETES__NAMESPACE", "default"),
                        is_delete_operator_pod=True, # Self-cleans pods to save memory space
                        log_events_on_failure=True,
                        get_logs=True
                    )
                    
                    if previous_task:
                        previous_task >> current_task
                        
                    dbt_tasks.append(current_task)
                    previous_task = current_task

                return dbt_tasks
                    
                

        
        start >> ingestion_task >> precheck_file >> [file_exist, file_not_exist] 
        file_exist >> dbt_load_entity_level(dbt_load, entity_name)>> end




main()