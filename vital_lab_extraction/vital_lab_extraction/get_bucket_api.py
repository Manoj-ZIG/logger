import os
import pandas as pd
from io import BytesIO, StringIO

def get_bucket_api(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key):
    print(f"Reading zai_audit_process_params -'{parameters_bucket_name}' - '{zai_audit_process_params_key}'")
    csv_life_cycle_obj = s3_c.get_object(
        Bucket=parameters_bucket_name, Key = zai_audit_process_params_key)
    csv_life_cycle_body = csv_life_cycle_obj['Body']
    csv_life_cycle_string = csv_life_cycle_body.read().decode('utf-8')
    csv_life_cycle = pd.read_csv(StringIO(csv_life_cycle_string), encoding='latin', dtype=str)
    
    csv_life_cycle['is_process'] = csv_life_cycle['is_process'].astype('int32')
    csv_life_cycle['is_prod_api'] = csv_life_cycle['is_prod_api'].astype('int32')
    csv_life_cycle['is_prod_bucket'] = csv_life_cycle['is_prod_bucket'].astype('int32')
    csv_life_cycle['is_lifecycle'] = csv_life_cycle['is_lifecycle'].astype('int32')
    
    csv_life_cycle = csv_life_cycle[csv_life_cycle['is_process'] == 1]
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(
        lambda x: x.replace('.pdf', ''))

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
    # else:
    #     bucket_name = os.environ['BUCKET_NAME']
    #     save_path = os.environ['SAVE_PATH']
    #     api_endpoint_url = os.environ['API_URL']

    return bucket_name, api_endpoint_url, save_path
