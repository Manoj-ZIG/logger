import os
import json
import boto3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

def read_csv_file(s3c, bucket_name_const, key, encoding, dtype = None):
    """
    ### The function reads the claim template map constant file
    args:
        s3_c: s3_client
        bucket_name: bucket_name
        key: key
    return:
        df: DataFrame
    """
    path_for_csv = key
    csv_obj = s3c.get_object(
        Bucket=bucket_name_const, Key=rf"{path_for_csv}")
    csv_body = csv_obj['Body']
    csv_string = csv_body.read().decode('utf-8')
    if dtype:
        csv_data = pd.read_csv(StringIO(csv_string),encoding=encoding, dtype=str)
    else:
        csv_data = pd.read_csv(StringIO(csv_string), encoding=encoding)
    return csv_data

def get_zai_emr_system_name_version(grd_file, zai_provider_emr_mapping):
    try:
        billing_provider_tin = grd_file['billing_provider_tin'].iloc[0] if grd_file['billing_provider_tin'].iloc[0] != '' else None
    except:
        billing_provider_tin = None
    try:
        billing_provider_npi = grd_file['billing_provider_npi'].iloc[0] if grd_file['billing_provider_npi'].iloc[0] != '' else None
    except:
        billing_provider_npi = None
    if billing_provider_tin and billing_provider_tin in zai_provider_emr_mapping['provider_tin'].to_list():
        if billing_provider_npi and billing_provider_npi in zai_provider_emr_mapping['provider_npi'].to_list():
            zai_emr_system_name = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == billing_provider_tin) & \
                                                        (zai_provider_emr_mapping["provider_npi"] == billing_provider_npi)].iloc[0]['zai_emr_system_name']
            zai_emr_system_version = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == billing_provider_tin) & \
                                                        (zai_provider_emr_mapping["provider_npi"] == billing_provider_npi)].iloc[0]['zai_emr_system_version']
        else:
            zai_emr_system_name = zai_provider_emr_mapping[zai_provider_emr_mapping["provider_tin"] == billing_provider_tin].iloc[0]['zai_emr_system_name']
            zai_emr_system_version = zai_provider_emr_mapping[zai_provider_emr_mapping["provider_tin"] == billing_provider_tin].iloc[0]['zai_emr_system_version']
    else:
        zai_emr_system_name = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == "111111111") & \
                                                        (zai_provider_emr_mapping["provider_npi"] == "1111111111")].iloc[0]['zai_emr_system_name']
        zai_emr_system_version = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == "111111111") & \
                                                        (zai_provider_emr_mapping["provider_npi"] == "1111111111")].iloc[0]['zai_emr_system_version']
    return zai_emr_system_name, zai_emr_system_version

def lifecycle_file_generator(s3_c, bucket_name, save_path, client_claim_id, zigna_claim_id, client_initial_id, template_name, status, stage, datetime_const, query_name, sub_status=""):
    
    folder_name = f"{datetime_const}"
    batch_id = f"{datetime_const}"
    load_date = f"{datetime_const}"
    text = 'batch_id|client_claim_id|zigna_claim_id|client_initial_id|status|sub_status|load_date|stage|template_name|query_name|' + "\n"
    text += f"{batch_id}|{client_claim_id}|{zigna_claim_id}|{client_initial_id}|{status}|{sub_status}|{load_date}|{stage}|{template_name}|{query_name}|"
    print(text)
    s3_c.put_object(Bucket=bucket_name, Body=str(text),
                    Key=rf"{save_path}/{folder_name}/devoted_{stage}_{zigna_claim_id}_{folder_name}.txt")
    return rf"{save_path}/{folder_name}/devoted_{stage}_{zigna_claim_id}_{folder_name}.txt"

def send_payload(api_endpoint_url, file_path, access_token, mr_name, job_type, comments, payload='', added_by='', notify_to='', should_notify='', job_status='pending'):
    api_url = api_endpoint_url
    return_payload = {
        "filePath": f"/{file_path}",
        "job_type": job_type,
        "job_status": job_status,
        "payload": payload,
        "comments": comments,
        "added_by": added_by,
        "notify_to": notify_to,
        "should_notify": should_notify,
        "medical_record_name": mr_name
    }
    # response = requests.post(api_url, json=return_payload)
    response = requests.post(api_endpoint_url,
      headers={'Authorization': 'Bearer {}'.format(access_token)}, json=return_payload)
    print(f'status_code: {response.status_code} |{response.json()}')

    return response.status_code

def send_payload_lifecycle(api_endpoint_url, file_path, access_token, job_type, comments, payload='', added_by='', notify_to='', should_notify='', job_status='pending'):
    api_url = api_endpoint_url
    return_payload = {
        "filePath": f"/{file_path}",
        "job_type": job_type,
        "job_status": job_status,
        "payload": payload,
        "comments": comments,
        "added_by": added_by,
        "notify_to": notify_to,
        "should_notify": should_notify
    }
    # response = requests.post(api_url, json=return_payload)
    response = requests.post(api_endpoint_url,
      headers={'Authorization': 'Bearer {}'.format(access_token)}, json=return_payload)
    print(f'status_code: {response.status_code} |{response.json()}')

    return response.status_code

def get_bucket_api(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key):
    print(f"Reading zai_audit_process_params -'{parameters_bucket_name}' - '{zai_audit_process_params_key}'")
    csv_life_cycle = read_csv_file(s3_c, parameters_bucket_name, zai_audit_process_params_key, encoding='latin', dtype = str)
    csv_life_cycle['is_process'] = csv_life_cycle['is_process'].astype('int32')
    csv_life_cycle['send_lifecycle_stage1_2_3'] = csv_life_cycle['send_lifecycle_stage1_2_3'].astype('int32')
    csv_life_cycle['send_lifecycle_stage4'] = csv_life_cycle['send_lifecycle_stage4'].astype('int32')
    csv_life_cycle['is_prod_api'] = csv_life_cycle['is_prod_api'].astype('int32')
    csv_life_cycle['is_prod_bucket'] = csv_life_cycle['is_prod_bucket'].astype('int32')
    csv_life_cycle['is_lifecycle'] = csv_life_cycle['is_lifecycle'].astype('int32')
    csv_life_cycle['is_zigna_selected'] = csv_life_cycle['is_zigna_selected'].astype('int32')
    csv_life_cycle = csv_life_cycle[csv_life_cycle['is_process'] == 1]
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(
        lambda x: str(x).replace('.pdf', ''))
    try:
        data_dict = dict(zip(csv_life_cycle.columns, csv_life_cycle[csv_life_cycle['renamed_medical_record_name'] == file_name.replace('.csv', '')].values[0]))
    except Exception as e:
        print(f"NO SUCH FILE FOUND NAMED : {file_name}")
        print(e)
    environment = os.environ["PROCESSING_ENVIRONMENT"]
    if csv_life_cycle[csv_life_cycle['renamed_medical_record_name'] == file_name.replace('.csv', '')].shape[0]>0:
        
        if environment == "prod" and client_name == 'devoted':
            bucket_name = os.environ["BUCKET_NAME"]
            save_path = f"{client_name}/" + os.environ["SAVE_PATH"]
            api_endpoint_url = 'https://api.revmaxai.com/policy/insert_task_into_queue'
        
        elif environment == "prod" and client_name == 'helix':
            bucket_name = os.environ["BUCKET_NAME"]
            save_path = f"{client_name}/" + os.environ["SAVE_PATH"]
            api_endpoint_url = 'https://api-helix.revmaxai.com/policy/insert_task_into_queue'

        elif environment == "uat" and client_name == 'devoted':
            bucket_name = os.environ["BUCKET_NAME"]
            save_path = os.environ["SAVE_PATH"]
            api_endpoint_url = os.environ["API_URL"]

        elif environment == "uat" and client_name == 'helix':
            bucket_name = "zai-revmax-demo"
            save_path = "helix/audits/medical_records"
            api_endpoint_url = 'https://api-helix-p.revmaxai.com/policy/insert_task_into_queue'

        elif environment == "qa" and client_name == 'devoted':
            bucket_name = os.environ["BUCKET_NAME"]
            save_path = os.environ["SAVE_PATH"]
            api_endpoint_url = os.environ["API_URL"]

        elif environment == "qa" and client_name == 'helix':
            bucket_name = "zai-revmax-demo"
            save_path = "helix/audits/medical_records"
            api_endpoint_url = 'https://api-helix-p.revmaxai.com/policy/insert_task_into_queue'

    return bucket_name, api_endpoint_url, save_path, data_dict

def get_auth_token(secretpath):
    auth_token = ''
    secret_name = secretpath
    try:
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_values = response['SecretString']
        credentials = json.loads(secret_values)
        print("Fetching credentials from secret manager successful")
        userId = credentials['userId']
        endpoint = credentials['endpoint']
        apiKey = credentials['apiKey']
        headers = {
        "X-API-Key": apiKey
        }

        payload = {
            "email": userId
            }
        res = requests.post(f"{endpoint}/getAuthKeyToken", json = payload, headers = headers)
        auth_token = res.json()["accessToken"]
        print("Fetching auth token successful")
    except Exception as e:
        print(f"Error during api call: {e}")
    return auth_token

def calculate_missing_section_percentage(file_name, df, df_co):
    '''
    ### Calculates the missing section percentage
    
    args:
        file_name: string (file name)
        df: DataFrame
        df_co: DataFrame (chart order)
    return:
        None
    '''
    detected_sections_page_list = []
    if not df_co.empty:
        for i, row in df_co.iterrows():
            try:
                start_page = int(row['min_page'])
                end_page = int(row['max_page'])+1
                detected_sections_page_list.extend(list(range(start_page, end_page)))
            except:
                pass
    total_page_ls = sorted(list(set(df['Page'].to_list()))) if 'Page' in df.columns else []
    missing_section_page_list = set(total_page_ls).difference(set(detected_sections_page_list))
    total_pages = df['Page'].max() if not df.empty and df['Page'].max() != 0 else 1
    missing_section_percentage = (len(missing_section_page_list)/total_pages)*100
    print(f"Missing Section Percentage for {file_name} - {round(missing_section_percentage, 2)}")