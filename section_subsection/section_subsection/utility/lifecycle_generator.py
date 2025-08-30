import os
import re
import json
import boto3
# import logging
import warnings
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
warnings.filterwarnings("ignore")
# logging.basicConfig(level=logging.DEBUG,
#                             format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
try:
    from utils import send_payload_lifecycle, get_bucket_api, get_auth_token, read_csv_file
except:
    from .utils import send_payload_lifecycle, get_bucket_api, get_auth_token, read_csv_file

secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

def stage_1(csv_life_cycle, s3_c, bucket_name, api_endpoint_url, save_path, datetime_const, client_name, status, stage):
    # logging.basicConfig(level=logging.DEBUG,
    #                         format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    # logging.info("FUNCTION START: stage_1")
    print("FUNCTION START: stage_1")

    folder_name = f"{datetime_const}"
    load_date = f"{datetime_const}"
    # logging.info(f"FUNCTION START: stage_1 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    print(f"FUNCTION START: stage_1 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    batch_no = csv_life_cycle.iloc[0]['batch_no']
    text = 'load_date|client_claim_id|zai_audit_id|client_initial_id|zai_claim_key|status|sub_status|stage|template_name|query_name|' + "\n"
    for i in range(len(csv_life_cycle)):
        sub_status = ""
        client_claim_id = csv_life_cycle.iloc[i]['claim_id']
        zai_audit_id = csv_life_cycle.iloc[i]['cleaned_zigna_claim_id'] if csv_life_cycle.iloc[i]['is_zigna_selected'] == 1 else csv_life_cycle.iloc[i]['zigna_claim_id']
        client_initial_id = csv_life_cycle.iloc[i]['adjudication_record_locator']
        zai_claim_key =  csv_life_cycle.iloc[i]['root_payer_control_number']
        detected_template_ = csv_life_cycle.iloc[i]['template_name']
        query_name = csv_life_cycle.iloc[i]['query_name'] if str(csv_life_cycle.iloc[i]['query_name']).lower() != 'nan' else ""
        mapped_template = {'AKI': 'Acute Kidney Injury', 'ATN': 'ATN',
                       'PNA': 'Pneumonia', 'MISC': 'Miscellaneous', 'AMI': 'AMI', 'SEPSIS':'SEPSIS'}
        if detected_template_ and detected_template_ in mapped_template.keys():
            template_name = mapped_template.get(detected_template_)
        else:
            template_name = 'Miscellaneous'
        text += f"{load_date}|{client_claim_id}|{zai_audit_id}|{client_initial_id}|{zai_claim_key}|{status}|{sub_status}|{stage}|{template_name}|{query_name}|" + "\n"
    s3_c.put_object(Bucket=bucket_name, Body=str(text),
                    Key=rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt")
    # logging.info(f"FUNCTION START: stage_1 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    print(f"FUNCTION START: stage_1 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    return rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt"

def stage_2(csv_life_cycle, s3_c, bucket_name, api_endpoint_url, save_path, datetime_const, client_name, status, stage):
    # logging.basicConfig(level=logging.DEBUG,
    #                         format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    # logging.info("FUNCTION START: stage_2")
    print("FUNCTION START: stage_2")
    folder_name = f"{datetime_const}"
    load_date = f"{datetime_const}"
    # logging.info(f"FUNCTION START: stage_2 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    print(f"FUNCTION START: stage_2 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    batch_no = csv_life_cycle.iloc[0]['batch_no']
    text = 'load_date|client_claim_id|zai_audit_id|client_initial_id|zai_claim_key|status|sub_status|stage|template_name|query_name|program|' + "\n"
    for i in range(len(csv_life_cycle)):
        client_claim_id = csv_life_cycle.iloc[i]['claim_id']
        zai_audit_id = csv_life_cycle.iloc[i]['cleaned_zigna_claim_id'] if csv_life_cycle.iloc[i]['is_zigna_selected'] == 1 else csv_life_cycle.iloc[i]['zigna_claim_id']
        client_initial_id = csv_life_cycle.iloc[i]['adjudication_record_locator']
        zai_claim_key =  csv_life_cycle.iloc[i]['root_payer_control_number']
        detected_template_ = csv_life_cycle.iloc[i]['template_name']
        query_name = csv_life_cycle.iloc[i]['query_name'] if str(csv_life_cycle.iloc[i]['query_name']).lower() != 'nan' else ""
        program_code = csv_life_cycle.iloc[i]['program_code'] if str(csv_life_cycle.iloc[i]['program_code']).lower() != 'nan' else ""
        sub_status = ""
        mapped_template = {'AKI': 'Acute Kidney Injury', 'ATN': 'ATN',
                       'PNA': 'Pneumonia', 'MISC': 'Miscellaneous', 'AMI': 'AMI', 'SEPSIS':'SEPSIS'}
        if detected_template_ and detected_template_ in mapped_template.keys():
            template_name = mapped_template.get(detected_template_)
        else:
            template_name = 'Miscellaneous'
        text += f"{load_date}|{client_claim_id}|{zai_audit_id}|{client_initial_id}|{zai_claim_key}|{status}|{sub_status}|{stage}|{template_name}|{query_name}|{program_code}|" + "\n"
    s3_c.put_object(Bucket=bucket_name, Body=str(text),
                    Key=rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt")
    # logging.info(f"FUNCTION START: stage_2 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    print(f"FUNCTION START: stage_2 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    return rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt"

def stage_3(csv_life_cycle, s3_c, bucket_name, api_endpoint_url, save_path, datetime_const, client_name, status, stage):
    # logging.basicConfig(level=logging.DEBUG,
    #                         format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    # logging.info("FUNCTION START: stage_3")
    print("FUNCTION START: stage_3")
    folder_name = f"{datetime_const}"
    load_date = f"{datetime_const}"
    # logging.info(f"FUNCTION START: stage_3 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    print(f"FUNCTION START: stage_3 : shape of csv_life_cycle : {csv_life_cycle.shape}")
    batch_no = csv_life_cycle.iloc[0]['batch_no']
    text = 'load_date|client_claim_id|zai_audit_id|client_initial_id|zai_claim_key|status|sub_status|stage|template_name|query_name|program|priority|' + "\n"
    for i in range(len(csv_life_cycle)):
        client_claim_id = csv_life_cycle.iloc[i]['claim_id']
        zai_audit_id =  csv_life_cycle.iloc[i]['cleaned_zigna_claim_id'] if csv_life_cycle.iloc[i]['is_zigna_selected'] == 1 else csv_life_cycle.iloc[i]['zigna_claim_id']
        client_initial_id = csv_life_cycle.iloc[i]['adjudication_record_locator']
        zai_claim_key =  csv_life_cycle.iloc[i]['root_payer_control_number']
        detected_template_ = csv_life_cycle.iloc[i]['template_name']
        query_name = csv_life_cycle.iloc[i]['query_name'] if str(csv_life_cycle.iloc[i]['query_name']).lower() != 'nan' else ""
        program_code = csv_life_cycle.iloc[i]['program_code'] if str(csv_life_cycle.iloc[i]['program_code']).lower() != 'nan' else ""
        sub_status = "final_selections"
        priority = csv_life_cycle.iloc[i]['priority'] if str(csv_life_cycle.iloc[i]['priority']).lower() != 'nan' else ""
        mapped_template = {'AKI': 'Acute Kidney Injury', 'ATN': 'ATN',
                       'PNA': 'Pneumonia', 'MISC': 'Miscellaneous', 'AMI': 'AMI', 'SEPSIS':'SEPSIS',"ENCEPHALOPATHY" : "Encephalopathy"}
        if detected_template_ and detected_template_ in mapped_template.keys():
            template_name = mapped_template.get(detected_template_)
        else:
            template_name = 'Miscellaneous'
        text += f"{load_date}|{client_claim_id}|{zai_audit_id}|{client_initial_id}|{zai_claim_key}|{status}|{sub_status}|{stage}|{template_name}|{query_name}|{program_code}|{priority}|" + "\n"
    s3_c.put_object(Bucket=bucket_name, Body=str(text),
                    Key=rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt")
    
    # logging.info(f"FUNCTION START: stage_3 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    print(f"FUNCTION START: stage_3 : file_path : {rf'{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt'}")
    return rf"{save_path}/{folder_name}/{client_name}_{stage}_{batch_no}_{folder_name}.txt"

def stage_4(s3_c, bucket_name, save_path, client_claim_id, zai_audit_id, client_initial_id, zai_claim_key, template_name, client_name, status, stage, datetime_const, query_name, program_code, priority, sub_status=""):

    folder_name = f"{datetime_const}"
    load_date = f"{datetime_const}"
    text = 'load_date|client_claim_id|zai_audit_id|client_initial_id|zai_claim_key|status|sub_status|stage|template_name|query_name|program|priority|' + "\n"
    text += f"{load_date}|{client_claim_id}|{zai_audit_id}|{client_initial_id}|{zai_claim_key}|{status}|{sub_status}|{stage}|{template_name}|{query_name}|{program_code}|{priority}|"
    s3_c.put_object(Bucket=bucket_name, Body=str(text),
                    Key=rf"{save_path}/{folder_name}/{client_name}_{stage}_{zai_audit_id}_{folder_name}.txt")

    return rf"{save_path}/{folder_name}/{client_name}_{stage}_{zai_audit_id}_{folder_name}.txt"

def get_stage_4_data(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path, access_token, status_, client_name):
    # logging.basicConfig(level=logging.DEBUG,
    #                         format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    # logging.info("FUNCTION START: get_stage_4_data")
    print("FUNCTION START: get_stage_4_data")
    # logging.info(f"FUNCTION START: get_stage_4_data : shape of csv_life_cycle : {csv_life_cycle.shape}")
    print(f"FUNCTION START: get_stage_4_data : shape of csv_life_cycle : {csv_life_cycle.shape}")
    for i in range(len(csv_life_cycle)):
        datetime_const = f"{datetime.now().strftime('%Y_%m_%d_%H%M%S')}"
        client_claim_id = csv_life_cycle.iloc[i]['claim_id']
        zai_audit_id = csv_life_cycle.iloc[i]['cleaned_zigna_claim_id'] if csv_life_cycle.iloc[i]['is_zigna_selected'] == 1 else csv_life_cycle.iloc[i]['zigna_claim_id']
        client_initial_id = csv_life_cycle.iloc[i]['adjudication_record_locator']
        zai_claim_key =  csv_life_cycle.iloc[i]['root_payer_control_number']
        detected_template_ = csv_life_cycle.iloc[i]['template_name']
        query_name = csv_life_cycle.iloc[i]['query_name'] if str(csv_life_cycle.iloc[i]['query_name']).lower() != 'nan' else ""
        program_code = csv_life_cycle.iloc[i]['program_code'] if str(csv_life_cycle.iloc[i]['program_code']).lower() != 'nan' else ""
        priority = csv_life_cycle.iloc[i]['priority'] if str(csv_life_cycle.iloc[i]['priority']).lower() != 'nan' else ""
        mapped_template = {'AKI': 'Acute Kidney Injury', 'ATN': 'ATN',
                        'PNA': 'Pneumonia', 'MISC': 'Miscellaneous', 'AMI': 'AMI', 'SEPSIS':'SEPSIS',"ENCEPHALOPATHY" : "Encephalopathy"}
        if detected_template_ and detected_template_ in mapped_template.keys():
            template_name = mapped_template.get(detected_template_)
        else:
            template_name = 'Miscellaneous'

        life_cyc_file_path4 = stage_4(s3_c, destination_bucket, save_path, client_claim_id, zai_audit_id,client_initial_id, zai_claim_key, template_name, client_name, status=status_, stage='04', datetime_const=datetime_const, query_name=query_name, program_code = program_code, priority = priority, sub_status="")
        # logging.info(f"FUNCTION START: get_stage_4_data : life_cyc_file_path4 : {life_cyc_file_path4}")
        print(f"FUNCTION START: get_stage_4_data : life_cyc_file_path4 : {life_cyc_file_path4}")
        status = send_payload_lifecycle(api_endpoint_url,life_cyc_file_path4, access_token, 'auditDrg_lifeCycle', 'stage_04', '', '', '', '', job_status='pending')
        # print(i, datetime_const,"|||", client_claim_id)

def process(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key):
    # logging.basicConfig(level=logging.DEBUG,
    #                         format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    # logging.info("FUNCTION START: process")
    print(f"FUNCTION START: process : {datetime.now().strftime('%Y_%m_%d_%H%M%S')}")
    print("FUNCTION START: process")
    destination_bucket, api_endpoint_url, save_path, _ = get_bucket_api(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key)
    secret_path = os.environ['SECRET_PATH']
    access_token = get_auth_token(secret_path)
    datetime_const = f"{datetime.now().strftime('%Y_%m_%d_%H%M%S')}"
    # csv_life_cycle = read_csv(s3_c, file_name, parameters_bucket_name, zai_audit_process_params_key)
    
    csv_life_cycle = read_csv_file(s3_c, parameters_bucket_name, zai_audit_process_params_key, encoding = 'latin', dtype = str)
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(lambda x: str(x).replace('.pdf', ''))
    csv_life_cycle = csv_life_cycle[csv_life_cycle['renamed_medical_record_name'] == file_name.replace('.csv', '')]
    csv_life_cycle['is_process'] = csv_life_cycle['is_process'].astype('int32')
    csv_life_cycle['send_lifecycle_stage1_2_3'] = csv_life_cycle['send_lifecycle_stage1_2_3'].astype('int32')
    csv_life_cycle['send_lifecycle_stage4'] = csv_life_cycle['send_lifecycle_stage4'].astype('int32')
    csv_life_cycle['is_prod_api'] = csv_life_cycle['is_prod_api'].astype('int32')
    csv_life_cycle['is_prod_bucket'] = csv_life_cycle['is_prod_bucket'].astype('int32')
    csv_life_cycle['is_lifecycle'] = csv_life_cycle['is_lifecycle'].astype('int32')
    csv_life_cycle['is_zigna_selected'] = csv_life_cycle['is_zigna_selected'].astype('int32')

    if csv_life_cycle.shape[0] > 0:
        if csv_life_cycle['send_lifecycle_stage1_2_3'].iloc[0] == 1:
            # life_cyc_file_path1 = stage_1(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path, datetime_const, status="Data Ingested", stage = '01',)
            # send_payload_lifecycle(api_endpoint_url, life_cyc_file_path1, access_token,  'auditDrg_lifeCycle', 'stage_01', '', '', '', '', job_status='pending')
            
            # access_token = get_auth_token(secret_path)
            # life_cyc_file_path2 = stage_2(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path, datetime_const, status="Claim Selected", stage = '02',)
            # send_payload_lifecycle(api_endpoint_url, life_cyc_file_path2, access_token, 'auditDrg_lifeCycle', 'stage_02', '', '', '', '', job_status='pending')

            access_token = get_auth_token(secret_path)
            life_cyc_file_path3 = stage_3(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path, datetime_const, client_name, status="claim_finalized", stage = '03')
            send_payload_lifecycle(api_endpoint_url, life_cyc_file_path3, access_token, 'auditDrg_lifeCycle', 'stage_03', '', '', '', '', job_status='pending')
        
        if csv_life_cycle['send_lifecycle_stage4'].iloc[0] == 0:
            access_token = get_auth_token(secret_path)
            status_ = "IBR Received"
            get_stage_4_data(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path ,access_token, status_, client_name)
        
        if csv_life_cycle['send_lifecycle_stage4'].iloc[0] == 1:
            access_token = get_auth_token(secret_path)
            status_ = "MR Received"
            get_stage_4_data(csv_life_cycle, s3_c, destination_bucket, api_endpoint_url, save_path ,access_token, status_, client_name)
        
        # logging.info("FUNCTION END  : get_bucket_api")
        print("FUNCTION END  : get_bucket_api")
        print(f"FUNCTION END  : process : {datetime.now().strftime('%Y_%m_%d_%H%M%S')}")
    else:
        print(f"FUNCTION END  : process : No such file exists : {file_name}")
        print(f"FUNCTION END  : process : {datetime.now().strftime('%Y_%m_%d_%H%M%S')}")
