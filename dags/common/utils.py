
import os

import yaml
from airflow.exceptions import AirflowException, AirflowSkipException


def read_local_file_contents_yml(full_file_path:str):
    try:
        status = 200
        with open(full_file_path, 'r') as file:
            file_content = yaml.safe_load(file)
    except Exception as e:
        print(e)
        status = 400
        file_content = None
    return status, file_content



def profile_file_full_path(master_config_path:str, data_source:str):
    return f"{master_config_path}/{data_source}_profile.yml"
    
        
def get_datasource_profile(config_path:str, env:str, data_source:str):
    profile_file_path = profile_file_full_path(config_path, data_source)
    
    status, list_profile = read_local_file_contents_yml(profile_file_path)

    if env in list_profile.keys():
        dict_profiles = list_profile[env]
    else:
        dict_profiles = list_profile[list(list_profile.keys())[0]]

    header_row = list(dict_profiles)[0]

    header_keys = []
    for key, value in header_row.items():
        header_keys.append(f"{key}")

    if status == 400:
        raise AirflowException("[DAG]: Read Data source profile failed")
    return header_keys, dict_profiles