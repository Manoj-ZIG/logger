import json
import logging
import boto3
import pandas as pd
import numpy as np
import urllib.parse
from io import BytesIO, StringIO
import os
import warnings
import sys
from dateutil.parser import parse
warnings.filterwarnings("ignore")
try:
    from utility.textract_response import TextractTableResponse
    from utility.textract_response_parsing import TextractTableResponse as TextractTableResponseParsing
    # from utility.table_merger import TableMerger
    from utility.table_merger_v2 import TableMerger #
    from utility.table_parser import TableParser
    from utility.get_result import GetResult
    from utility.read_csv_from_s3 import read_csv_from_s3, copy_object_to_s3, read_csv_file
    from utility.get_zai_emr_system_name_version import get_zai_emr_system_name_version
    from utility.get_json_data import JsonData
    from utility.get_api import send_payload
    from utility.get_access_token import get_auth_token
    from utility.get_bucket_api import get_bucket_api
    from utility.data_loss_calculation import save_data_loss_log
    from utility.key_exist import key_exists
    from analysis.pneumonia_analytics import PneumoniaAnalysis
    from analysis.sepsis_analytics import SepsisAnalysis
    from analysis.aki_analytics import AKIAnalysis
    from analysis.ami_analytics import AMIAnalysis
    from analysis.sepsis_analytics import SepsisAnalysis
    from analysis.generic_analysis import GenericAnalysis
    from analysis.encephalopathy_analytics import EncephalopathyAnalysis

    from constant.postprocess_constant import adm_dis_tag
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from constant.analytics_constant import pneumonia_analytics_range, aki_analytics_range
    from constant.template_excerpt_constant import template_excerpt_data
    from postprocess.post_process import Postprocess
    from postprocess.get_postprocess_data_aki import GetPostProcessData as GetPostProcessDataAKI
    from postprocess.get_postprocess_data_pneumonia import GetPostProcessData as GetPostProcessDataPNM
    from postprocess.get_postprocess_data_generic import GetPostProcessData as GetPostProcessDataGeneric
    from postprocess.get_postprocess_data_ami import GetPostProcessData as GetPostProcessDataAMI
    from postprocess.get_postprocess_data_sepsis import GetPostProcessData as GetPostProcessDataSEPSIS
    from helpers.custom_logger import enable_custom_logging

except ModuleNotFoundError as e:
    from .utility.textract_response import TextractTableResponse
    from .utility.textract_response_parsing import TextractTableResponse as TextractTableResponseParsing
    # from .utility.table_merger import TableMerger
    from .utility.table_merger_v2 import TableMerger #
    from .utility.table_parser import TableParser
    from .utility.get_result import GetResult
    from .utility.read_csv_from_s3 import read_csv_from_s3, copy_object_to_s3, read_csv_file
    from .utility.get_zai_emr_system_name_version import get_zai_emr_system_name_version
    from .utility.get_json_data import JsonData
    from .utility.get_api import send_payload
    from .utility.get_access_token import get_auth_token
    from .utility.get_bucket_api import get_bucket_api
    from .utility.data_loss_calculation import save_data_loss_log
    from .utility.key_exist import key_exists
    from .analysis.pneumonia_analytics import PneumoniaAnalysis
    from .analysis.aki_analytics import AKIAnalysis
    from .analysis.ami_analytics import AMIAnalysis
    from .analysis.generic_analysis import GenericAnalysis
    from .analysis.sepsis_analytics import SepsisAnalysis
    from .analysis.encephalopathy_analytics import EncephalopathyAnalysis

    from .constant.postprocess_constant import adm_dis_tag
    from .constant.aws_config import aws_access_key_id, aws_secret_access_key
    from .constant.analytics_constant import pneumonia_analytics_range, aki_analytics_range
    from .constant.template_excerpt_constant import template_excerpt_data
    from .postprocess.post_process import Postprocess
    from .postprocess.get_postprocess_data_aki import GetPostProcessData as GetPostProcessDataAKI
    from .postprocess.get_postprocess_data_pneumonia import GetPostProcessData as GetPostProcessDataPNM
    from .postprocess.get_postprocess_data_generic import GetPostProcessData as GetPostProcessDataGeneric
    from .postprocess.get_postprocess_data_ami import GetPostProcessData as GetPostProcessDataAMI
    from .postprocess.get_postprocess_data_sepsis import GetPostProcessData as GetPostProcessDataSEPSIS
    from .helpers.custom_logger import enable_custom_logging

enable_custom_logging()
sns_topic_arn = "arn:aws:sns:us-east-1:833984991867:revmaxai_mr_data_processing_alarms"
sns_client = boto3.client("sns", region_name = "us-east-1")
def send_sns_message(messaage):
    response = sns_client.publish(
        TopicArn = sns_topic_arn,
        Message = messaage,
        Subject = "EC2 Notification"
    )
    print(f"Message sent! Message ID: {response['MessageId']}")


def lambda_handler(event, context):
    # bucket_name = event['bucket_name']
    # section_subsection_path = event['section_subsection_path']
    # path_to_save_result = event['path_to_save_result']
    # path_to_save_logs = f"{path_to_save_result}/logs"
    # detected_template = event['detected_template']

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    logging.getLogger('boto').setLevel(logging.ERROR)

    """ auto trigger"""
    if os.getenv("AWS_EXECUTION_ENV") is None:
        from dotenv import load_dotenv
        load_dotenv()
        print("Getting the environment variables form .env")
        
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    table_merged_json = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_name = table_merged_json.split('/')[-1].replace('_textract_table_merged.json', '')
    document_name=file_name+".pdf"
    # adj_tag = file_name.split('_')[0]
    client_name = table_merged_json.split("/")[0]
    path_to_save_result = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/tables"
    path_to_excerpt_result = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/excerpts"
    path_to_save_logs = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/logs/{file_name}"
    section_subsection_path = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/sectioned-data/{file_name.replace('_','-')}/{file_name.replace('.csv','')}_section_subsection.csv"
    ground_truth_const = f"{client_name}/" + os.environ["GROUND_TRUTH_CONST_PATH"]
    zai_audit_process_params_key =  f"{client_name}/" + os.environ["CLAIM_TEMPLATE_MAP_CONSTANT_PATH"] + f"zai_{client_name}_audit_process_params.csv"
    excerpts_regex_lab_list_disease_detection_params_key = os.environ["EXCERPTS_REGEX_LAB_LIST_DISEASE_DETECTION_PARAMS_PATH"]
    zai_lab_header_range_key = os.environ["ZAI_LAB_HEADER_RANGE_PATH"]
    zai_provider_emr_info_params_key = os.environ["ZAI_PROVIDER_EMR_INFO_PARAMS_PATH"]
    zai_provider_emr_mapping_threshold_table_key = os.environ["ZAI_PROVIDER_EMR_MAPPING_THRESHOLD_TABLE_PATH"]
    zai_lab_incl_excl_threshold_params_key = os.environ["ZAI_LAB_INCL_EXCL_THRESHOLD_PARAMS_PATH"]
    parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]

    data_loss_tracking_file = "data_loss_tracking_log.txt"

    # save_path_ui = f"sample_client/audits/medical_records"
    # save_path_ui_bucket = 'zai-revmax-test'
    # api_endpoint_url = 'https://253w8ijrh0.execute-api.us-east-1.amazonaws.com/Prod/policy/insert_task_into_queue'
    """------------------"""
    
    claim_id = file_name.split('_')[0]
    mr_name = file_name.replace(claim_id+"_", "")+".pdf"
    print(f'processing: {file_name}| claim id: {claim_id}')
    print(f"s3_key : {table_merged_json}")
    print('variable loading..')
    try:   
        s3_c = boto3.client('s3', region_name='us-east-1')
        s3_c.list_buckets()
        print("S3 client initialized successfully using IAM role in app.py.")
    except Exception as e:
        print(f"Failed to initialize S3 client with IAM role: {str(e)} in app.py.")
        if aws_access_key_id and aws_secret_access_key:
            s3_c = boto3.client('s3', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
            print("S3 client initialized successfully using manual keys in app.py.")
        else:
            raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in app.py.")
    try:
        s3_r = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        s3_r.buckets.all()
        print("S3 resource initialized using IAM role in app.py.")
    except Exception as e:
        if aws_access_key_id and aws_secret_access_key:
            s3_r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            print("S3 resource initialized using manual keys in app.py.")
        else:
            raise Exception("S3 resource initialization failed. Check IAM role or AWS credentials in app.py.")

    save_path_ui_bucket, api_endpoint_url, save_path_ui = get_bucket_api(
        s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key)
    ground_truth_const_ = f'{ground_truth_const}{file_name.replace(".json","")}_ground_truth_constants.csv'
    
    print(f"Reading ground_truth_const -'{parameters_bucket_name}' - '{ground_truth_const_}'")
    grd_obj = s3_c.get_object(
        Bucket=parameters_bucket_name, Key=ground_truth_const_)
    grd_body = grd_obj['Body']
    grd_body_string = grd_body.read().decode('utf-8')
    grd_df = pd.read_csv(StringIO(grd_body_string), dtype=str)
    adm_date = grd_df['admission_date'].iloc[0]
    disch_date = grd_df['discharge_date'].iloc[0]
    min_max_date = (str(adm_date), str(disch_date))

    #4 Read the zai_provider_emr_info_params file
    print(f"Reading zai_provider_emr_info_params -'{parameters_bucket_name}' - '{zai_provider_emr_info_params_key}'")
    zai_provider_emr_info_params = read_csv_file(s3_c, parameters_bucket_name, zai_provider_emr_info_params_key, encoding='latin', dtype = str)
    zai_emr_system_name, zai_emr_system_version = get_zai_emr_system_name_version(grd_df, zai_provider_emr_info_params)
    
    #4 Read the zai_provider_emr_threshold_params threshold file
    print(f"Reading zai_provider_emr_mapping_threshold_table_key -'{parameters_bucket_name}' - '{zai_provider_emr_mapping_threshold_table_key}'")
    zai_provider_emr_threshold_params = read_csv_file(s3_c, parameters_bucket_name, zai_provider_emr_mapping_threshold_table_key, encoding='latin')
    zai_provider_emr_threshold_params['zai_emr_system_version'] =  zai_provider_emr_threshold_params['zai_emr_system_version'].astype('int32')
    zai_provider_emr_mapping_threshold_table_dict = zai_provider_emr_threshold_params[(zai_provider_emr_threshold_params['zai_emr_system_name'] == zai_emr_system_name) & \
                                             (zai_provider_emr_threshold_params['zai_emr_system_version'] == int(zai_emr_system_version))][['rule_name', 'rule_value']].set_index('rule_name').to_dict()
    # load claim_map file
    # claim_template_object = s3_c.get_object(Bucket=bucket_name,
    #                                         Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/claim_template_map_constant.json')
    # claim_template_body = claim_template_object["Body"].read().decode(
    #     'utf-8')
    # claim_template_map = json.loads(claim_template_body)

    print(f"Reading zai_audit_process_params -'{parameters_bucket_name}' - '{zai_audit_process_params_key}'")
    csv_life_cycle_obj = s3_c.get_object(
        Bucket=parameters_bucket_name, Key=zai_audit_process_params_key)
    csv_life_cycle_body = csv_life_cycle_obj['Body']
    csv_life_cycle_string = csv_life_cycle_body.read().decode('utf-8')
    csv_life_cycle = pd.read_csv(StringIO(csv_life_cycle_string), encoding='latin', dtype=str)
    
    csv_life_cycle['is_process'] = csv_life_cycle['is_process'].astype('int32')
    csv_life_cycle = csv_life_cycle[csv_life_cycle['is_process'] == 1]
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(
        lambda x: x.replace('.pdf', ''))
    detected_template_ = csv_life_cycle[csv_life_cycle['renamed_medical_record_name']
                                        == file_name.replace('.csv', '')]['template_name'].iloc[0]

    # mapping the detected template
    mapped_template = {'AKI': 'aki', 'ATN': 'aki',
                       'PNA': 'pneumonia', 'MISC': 'generic', 'AMI': 'ami', "SEPSIS": 'sepsis', "ENCEPHALOPATHY" : "Encephalopathy"}
    detected_template = mapped_template.get('MISC')
    # detected_template_ = claim_template_map.get(claim_id)
    # if detected_template_ and detected_template_ in mapped_template.keys():
    #     detected_template = mapped_template.get(detected_template_)
    # else:
    #     detected_template = 'generic'
    print(f"detected template: {detected_template}")
    print(f"min_max_date: {min_max_date}")
    # Load the constant files
    print(f"Reading excerpts_regex_lab_list_disease_detection_params -'{parameters_bucket_name}' - '{excerpts_regex_lab_list_disease_detection_params_key}'")
    template_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                               Key=excerpts_regex_lab_list_disease_detection_params_key)
    template_constant_body = template_constant_object["Body"].read().decode('utf-8')
    template_constant = json.loads(template_constant_body)
    
    print(f"Reading zai_lab_header_range -'{parameters_bucket_name}' - '{zai_lab_header_range_key}'")
    lab_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                          Key=zai_lab_header_range_key)
    lab_constant_body = lab_constant_object["Body"].read().decode('utf-8')
    lab_constant = json.loads(lab_constant_body)

    print(f"Reading zai_lab_incl_excl_threshold_params -'{parameters_bucket_name}' - '{zai_lab_incl_excl_threshold_params_key}'")
    component_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                                Key=zai_lab_incl_excl_threshold_params_key)
    component_constant_body = component_object["Body"].read().decode('utf-8')
    zai_lab_incl_excl_threshold_params = json.loads(component_constant_body)

    #### variable loading 
    detected_labs = template_constant.get(
        detected_template).get('lab_list')
    template_analytics_dict = template_constant.get(
        detected_template).get('template_analytics_dict')
    suppress_attribute_ls = template_constant.get(
        detected_template).get('suppress_attribute_list_for_flag')
    attribute_name_list = template_constant.get(
        detected_template).get('attribute_dict').keys() - template_constant.get(
        detected_template).get('attribute_regex_dict') - set(suppress_attribute_ls)
    regex_attribute_list = list(template_constant.get(detected_template).get('attribute_regex_dict').keys())

    
    page_list_ = []
    logging.info('FUNCTION START: textract_response_parsing.py')
    print(f"json to csv conversion started:- {document_name}")
    reso = s3_c.get_object(Bucket=bucket_name,
                     Key= f'{path_to_save_logs}/table_page_list_log.txt')
    page_list_file = reso["Body"].read().decode('utf-8')
    for pg_ls in eval(page_list_file).values():
        page_list_ += pg_ls
    
    page_list = sorted(list(set(page_list_)))
   
    raw_data_df_list = []
    if len(page_list)>0 :
        #### Textract response (output from L3-4) ####
        print('read textract json..')
        txtract_obj = TextractTableResponseParsing(s3_client=s3_c)
        # with open(rf"E:\NLP_revMaxAI\scripts\all_entity\tests\pnm_pipeline_test_1201\261509502.395447442.H64425084\261509502.395447442.H64425084_mergedtable.json", 'r') as f:
        #     response = json.load(f)
        # merged_table_js_object = s3_c.get_object(Bucket=bucket_name,
        #                                          Key=f'{path_to_save_result}/{file_name}_mergedtable.json')
        merged_table_js_object = s3_c.get_object(Bucket=bucket_name,
                                                 Key=table_merged_json)
        merged_table_js_body = merged_table_js_object["Body"].read().decode(
            'utf-8')
        response = json.loads(merged_table_js_body)
        
        # save the table csv
        print('saving table csv..')
        logging.info(
            f'FUNCTION START: saving extracted table csv files...')
        table_meta_data = txtract_obj.get_textract_table_csv(
            response, file_name , page_list, bucket_name, f'{path_to_save_result}/lab-raw-data/{file_name}/table-csv')
        s3_c.put_object(Bucket=bucket_name, Body=str(table_meta_data),
                        Key=rf"{path_to_save_logs}/table_bb_log_info.txt")
        logging.info(
            f'FUNCTION END: saved extracted table csv files...')
        print(f"json to csv conversion completed:- {document_name}")
        
        # merging module
        # 1. reading all the table csv from s3 bucket and order them by pageNo
        print('raw result>> (merging table+parsing)..')
        print(f"merging tables and parsing started:- {document_name}")
        paginator = s3_c.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=f'{path_to_save_result}/lab-raw-data/{file_name}/table-csv/')

        table_csv_list = []
        for page in pages :
            if 'Contents' in page.keys():
                table_csv_list.extend([keys_['Key'] for keys_ in page['Contents'] ])

        if len(table_csv_list) > 0 :          
            table_csv_list_ordered = sorted(table_csv_list, key=lambda x: int(
                x.split("/")[-1].split("_")[0]))
            table_csv_df_map_dict = {k.split("/")[-1]:read_csv_from_s3(s3_c,bucket_name,k,na_filter=True)
                                    for k in table_csv_list_ordered}

            # 2. detect the lab and call the merger class
            lab_metadata_file_resp = s3_c.list_objects(
                Bucket=bucket_name,
                Prefix=path_to_save_logs)
            lab_metadata_file_list = [keys_['Key']
                                    for keys_ in lab_metadata_file_resp['Contents']]

            detected_labs = ['Vital', 'CBC', 'CMP'] + detected_labs
            sec_subsec_df = read_csv_from_s3(s3_c,bucket_name, section_subsection_path)
            table_pattern_logs = {}
            for lab in detected_labs:
                merged_table_metadata_lab = TableMerger(s3_c,
                                                        bucket_name,
                                                        table_csv_df_map_dict.copy(),
                                                        table_csv_list_ordered,
                                                        lab_constant.get(lab).get('lab_master_list'),
                                                        table_meta_data,
                                                        zai_provider_emr_mapping_threshold_table_dict).get_merger_meta_data()
                
                logging.info(
                    f'FUNCTION START: extract values for lab tables, table_parser.py')
                TableParserObj = TableParser(
                    lab_constant.get(lab).get('lab_master_list'),
                    lab_constant.get(lab).get('lab_alias_dict'), 
                    lab_constant.get(lab).get('lab_unit_list'))
                
                lab_table_page_meta_file = list(filter(lambda x: x.split("/")[-1].startswith(f'{lab}_table_page_log'), lab_metadata_file_list))
                if lab_table_page_meta_file:
                    
                    getResultObj = GetResult(s3_c,bucket_name, merged_table_metadata_lab, TableParserObj)
                    lab_result_list, pattern_logs = getResultObj.get_result(
                        lab_table_page_meta_file[0], sec_subsec_df,min_max_date)
                    lab_raw_df = getResultObj.save_result(
                        lab_result_list, f'{path_to_save_result}/lab-raw-data/{file_name}', str(lab))
                    table_pattern_logs[lab] = pattern_logs

                    raw_data_df_list.append(lab_raw_df)
                    print(f"appended raw result dataframe for {lab} to list")
            
                else:
                    print('lab metadata not found, possible reason: labs page not detected!')
            s3_c.put_object(Bucket=bucket_name, Body= json.dumps(table_pattern_logs),
                        Key=rf"{path_to_save_logs}/table_pattern_logs.json")
        else:
            print('no table csv file found!')
    else:
        print('no pages detected...')
    print(f"merging tables and parsing completed:- {document_name}")
    
    
    # load vital excerpt dataframe
    # result_file_resp = s3_c.list_objects(
    #     Bucket = bucket_name,
    #     Prefix=f'{path_to_excerpt_result}/vital_excerpt')
    # result_file_path = [keys_['Key'] for keys_ in result_file_resp['Contents']]
    # vital_excerpt_file_path = list(filter(lambda x: f'{file_name}_vitals' in x.split("/")
    #             [-1], result_file_path))
    vital_excerpt_file_path = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/excerpts/vital_excerpt/{file_name}_vitals.csv"
    if key_exists(s3_c, bucket_name, vital_excerpt_file_path):
        vital_excerpt_df = read_csv_from_s3(s3_c, bucket_name, vital_excerpt_file_path)
        vital_excerpt_df['BBInfo'] = vital_excerpt_df['BBInfo'].apply(lambda x: tuple(eval(x)))
        raw_data_df = pd.concat(raw_data_df_list+[vital_excerpt_df])
        raw_data_df.reset_index(inplace=True)
    else:
        raw_data_df = pd.concat(raw_data_df_list+[pd.DataFrame({})])
        raw_data_df.reset_index(inplace=True)
    
    print("raw df shape: ", raw_data_df.shape)
    # save data loss
    save_data_loss_log(s3_c, bucket_name,
                       f"{path_to_save_logs}/{data_loss_tracking_file}",
                       f"raw data table | {raw_data_df.shape} | 0%",
                       True
                       )
    # clean the raw_data
    if raw_data_df.shape[0] > 0:
        raw_data_df['TestName'] = raw_data_df['TestName'].apply(
            lambda x: str(x).strip())
        raw_data_df['TestResult'] = raw_data_df['TestResult'].apply(
            lambda x: str(x).strip())
        drop_df_index = raw_data_df[(raw_data_df['TestName'] == 'nan') |
                                    (raw_data_df['TestResult'] == 'nan') | (raw_data_df['TestResult'].isin(['-', '--']))].index
        raw_data_df.drop(drop_df_index, inplace=True)
        print("filtered raw df shape: ", raw_data_df.shape)
        save_data_loss_log(s3_c, bucket_name,
                        f"{path_to_save_logs}/{data_loss_tracking_file}",
                        f"raw data table filter| {raw_data_df.shape}",False
                        )

        # suppress the orders (table-data)
        # raw_data_df = raw_data_df[raw_data_df['MainSection'] != 'Orders']
        # raw_data_df = raw_data_df[~raw_data_df['Section'].isin(['Medication Administration', 'Medication Administration Summary', 'MAR', 'MAR with Pharmacy Actions'])]
        raw_data_df['IsDrop'] = 0
        raw_data_df.loc[raw_data_df[raw_data_df['MainSection'] == 'Orders'].index, 'IsDrop'] = 1
        raw_data_df.loc[raw_data_df[raw_data_df['Section'].isin(['Medication Administration', 'Medication Administration Summary', 'MAR', 'MAR with Pharmacy Actions'])].index, 'IsDrop'] = 1
        # raw_data_df.reset_index(inplace=True)
        save_data_loss_log(s3_c, bucket_name,
                        f"{path_to_save_logs}/{data_loss_tracking_file}",
                        f"raw data table order suppress| {raw_data_df[raw_data_df['IsDrop'] == 0].shape}", False
                        )

    # load template excerpts dataframe
    # excp_result_file_resp = s3_c.list_objects(
    #     Bucket=bucket_name,
    #     Prefix=f'{path_to_excerpt_result}/template_excerpt')
    # result_file_path = [keys_['Key'] for keys_ in excp_result_file_resp['Contents']]
    # exercpt_key = list(filter(lambda x: f'{file_name}_{detected_template}' in x.split("/")
    #                           [-1], result_file_path))
    exercpt_key = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/excerpts/template_excerpt/{file_name}_{detected_template}.csv"
    if key_exists(s3_c, bucket_name, exercpt_key):
        excerpt_result_df = read_csv_from_s3(
            s3_c, bucket_name, exercpt_key)
    else:
        excerpt_result_df = pd.DataFrame({})
        print('no excerpt data found...')
    print(f"Excerpt df: {excerpt_result_df.shape}")
    
    # suppress the orders (table-data)
    excerpt_result_df['IsDrop'] = 0
    if excerpt_result_df.shape[0]>0:
        # excerpt_result_df = excerpt_result_df[excerpt_result_df['MainSection'] != 'Orders']
        # excerpt_result_df = excerpt_result_df[~excerpt_result_df['Section'].isin(['Medication Administration', 'Medication Administration Summary', 'MAR', 'MAR with Pharmacy Actions'])]
        # excerpt_result_df.reset_index(inplace=True)
        excerpt_result_df.loc[excerpt_result_df[excerpt_result_df['MainSection'] == 'Orders'].index, 'IsDrop'] = 1
        excerpt_result_df.loc[excerpt_result_df[excerpt_result_df['Section'].isin(['Medication Administration', 'Medication Administration Summary', 'MAR', 'MAR with Pharmacy Actions'])].index, 'IsDrop'] = 1
    
    # postprocessing
    print('postprocessing..')
    print(f"post processing started:- {document_name}")
    logging.info(
        f'PROCESS START: postprocess start')
    textract_df = read_csv_from_s3(s3_c,bucket_name,section_subsection_path, usecols=['Text'])
    crp = " ".join(textract_df['Text'].apply(lambda x: str(x)))
    p_obj = Postprocess(adm_dis_tag)
    default_year = p_obj.get_adm_discharge_date(crp)
    try:
        default_year = parse(adm_date).year
    except ValueError as e:
        default_year = p_obj.get_adm_discharge_date(crp)
            
    logging.info(
        f'default year(adm/disch date): {default_year}')
    print('default_year: ', default_year)
    logging.info(
        f'FUNCTION START: get_postprocess_data.py, for raw_data: {raw_data_df.shape}')
    vital_reference_range = lab_constant.get(
        'Vital').get('lab_reference_range_dict')

    analysis_json_data = {}
    path_to_save_postprocess_result = f'{path_to_save_result}/postprocess-data/{file_name}'
    path_to_save_ui_data_result = f'{path_to_save_result}/ui-data/{file_name}'
    
    obj = GetPostProcessDataGeneric(
            template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
    postprocess_table_data, postprocess_table_result_path, \
        post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
                                                                                            default_year, raw_data_df, excerpt_result_df, attribute_name_list, regex_attribute_list, path_to_save_logs)
    if detected_template_ and detected_template_ in mapped_template.keys():
        detected_template = mapped_template.get(detected_template_)
    else:
        detected_template = 'generic'
    print(f"post processing completed:- {document_name}")

    print(f"analytics started:- {document_name}")
    template_analytics_dict = template_constant.get(detected_template).get('template_analytics_dict')
    suppress_attribute_ls = template_constant.get(detected_template).get('suppress_attribute_list_for_flag')
    attribute_name_list = template_constant.get(detected_template).get('attribute_dict').keys()
    vital_ls = [i[0] for i in lab_constant.get('Vital').get('lab_regex_list')]
    table_attribute_ls = list(template_analytics_dict.keys()) + vital_ls
    vital_regex_list = sorted(list(lab_constant.get('Vital').get('lab_reference_range_dict').keys()))
    
    if postprocess_table_data.shape[0] > 0:
        postprocess_table_data = postprocess_table_data[postprocess_table_data['TestName'].isin(table_attribute_ls)]
    if post_process_excerpt_df.shape[0] > 0:
        post_process_excerpt_df = post_process_excerpt_df[post_process_excerpt_df['TestName'].isin(attribute_name_list)]
    
    if 'index' in postprocess_table_data.columns:
        postprocess_table_data.drop(columns=['index'], inplace=True)
    postprocess_table_data = postprocess_table_data.reset_index()
    
    if 'index' in post_process_excerpt_df.columns:
        post_process_excerpt_df.drop(columns=['index'], inplace=True)
    post_process_excerpt_df = post_process_excerpt_df.reset_index()
    if detected_template == "pneumonia":
        # obj = GetPostProcessDataPNM(
        #     template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
        # postprocess_table_data, postprocess_table_result_path, \
        #     post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
        #                        default_year, raw_data_df, excerpt_result_df, attribute_name_list, path_to_save_logs)


        # Final JSON data result
        logging.info(
            f'FUNCTION START: template analysis,')
        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            pneumonia_analysis_obj = PneumoniaAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, vital_regex_list)
            analysis_json_data = pneumonia_analysis_obj.get_boolean_json_data(s3_c, bucket_name,
                postprocess_table_result_path,
                path_to_save_ui_data_result)
        logging.info(
            f'FUNCTION END: template analysis, analytics data keys: {len(analysis_json_data.keys())}')

    elif detected_template == 'aki':
        # obj = GetPostProcessDataAKI(
        #     template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
        # postprocess_table_data, postprocess_table_result_path, \
        #     post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
        #                        default_year, raw_data_df, excerpt_result_df, attribute_name_list, path_to_save_logs)
        
        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            aki_analysis_obj = AKIAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, vital_regex_list)
            analysis_json_data = aki_analysis_obj.get_boolean_json_data(s3_c,bucket_name,
                                                                        postprocess_table_result_path,
                                                                        path_to_save_ui_data_result)
    elif detected_template == 'ami':
        # obj = GetPostProcessDataAMI(
        #     template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
        # postprocess_table_data, postprocess_table_result_path, \
        #     post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
                            #    default_year, raw_data_df, excerpt_result_df, attribute_name_list, path_to_save_logs)
        
        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            ami_analysis_obj = AMIAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, vital_regex_list)
            analysis_json_data = ami_analysis_obj.get_boolean_json_data(s3_c,bucket_name,
                                                                        postprocess_table_result_path,
                                                                        path_to_save_ui_data_result)

    elif detected_template == 'sepsis':
        # obj = GetPostProcessDataSEPSIS(
        #     template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
        # postprocess_table_data, postprocess_table_result_path, \
        #      post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
        #                                                                                           default_year, raw_data_df, excerpt_result_df, attribute_name_list, path_to_save_logs)
        
        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            sepsis_analysis_obj = SepsisAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, vital_regex_list)
            analysis_json_data = sepsis_analysis_obj.get_boolean_json_data(s3_c,bucket_name,
                                                                        postprocess_table_result_path,
                                                                        path_to_save_ui_data_result)
    elif detected_template == "Encephalopathy":
        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            analysis_obj = EncephalopathyAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, vital_regex_list)
            analysis_json_data = analysis_obj.get_boolean_json_data(s3_c, bucket_name,
                                                                    postprocess_table_result_path,
                                                                    path_to_save_ui_data_result)
            
    elif detected_template == 'generic':
        # obj = GetPostProcessDataGeneric(
        #     template_analytics_dict, vital_reference_range,zai_lab_incl_excl_threshold_params)
        # postprocess_table_data, postprocess_table_result_path, \
        #     post_process_excerpt_df, post_process_excerpt_df_output_file = obj.get_postprocess_df(s3_c, bucket_name, path_to_save_postprocess_result, p_obj,
        #                                                                                           default_year, raw_data_df, excerpt_result_df, attribute_name_list, path_to_save_logs)

        if postprocess_table_data.shape[0] > 0 or post_process_excerpt_df.shape[0] > 0:
            analysis_obj = GenericAnalysis(
                postprocess_table_data, post_process_excerpt_df, template_analytics_dict, template_constant, vital_regex_list)
            analysis_json_data = analysis_obj.get_boolean_json_data(s3_c, bucket_name,
                                                                    postprocess_table_result_path,
                                                                    path_to_save_ui_data_result)
            
    else:
        print('not any detected template found')
    print(f"analytics completed:- {document_name}")

    print('final json data storing...')
    print(f'data files sending:- {document_name}')
    logging.info(
        f'FUNCTION END: template_analytics.py, analytics data keys: {len(analysis_json_data.keys())}')
    logging.info(
        f'FUNCTION START: get_json_data.py, saving the json result')
    
    # get the link of highlighted pdf file
    result_file_resp = s3_c.list_objects(
                                Bucket=bucket_name,
                                Prefix=path_to_save_result)
    result_file_path = [keys_['Key'] for keys_ in result_file_resp['Contents']]
    high_file_path = list(filter(lambda x: 'highlighted.pdf' in x.split("/")
                                [-1], result_file_path))
    
    # s3_url = f"{file_name}_highlighted.pdf" with claim_id
    s3_url = f"{file_name.replace(claim_id+'_','')}_highlighted.pdf"
    # save the json result
    jsDataObj = JsonData(s3_c, bucket_name)
    final_js_table, source_key_table = jsDataObj.get_table_json_data(
        claim_id, postprocess_table_data, path_to_save_ui_data_result)
    # copy_keyt = f"{save_path_ui}/{claim_id}/{source_key_table.get('Key').split('/')[-1]}" with_claim
    # secret_path = os.environ["SECRET_PATH"]
    # access_token = get_auth_token(secret_path)
    ################### UI-JSON Result Storing ###########################
    # """
    copy_keyt = f"{save_path_ui}/{claim_id}/{source_key_table.get('Key').split('/')[-1].replace(claim_id+'_','')}"
    copy_object_to_s3(s3_c, save_path_ui_bucket,source_key_table,copy_keyt )
    print(f'copy_source: { source_key_table} | copy_key: {copy_keyt} ')
    # status_code_lab_test = send_payload(
    #     api_endpoint_url, copy_keyt, access_token, mr_name, job_type='auditDrg_lab_tests', comments='lab test inserted')
    # print('lab_test', status_code_lab_test)

    final_js_excp, source_key_excerpt = jsDataObj.get_excerpt_json_data(
        claim_id, post_process_excerpt_df, path_to_save_ui_data_result, s3_url)
    copy_keye = f"{save_path_ui}/{claim_id}/{source_key_excerpt.get('Key').split('/')[-1].replace(claim_id+'_','')}"
    copy_object_to_s3(s3_c, save_path_ui_bucket,source_key_excerpt,copy_keye )
    print(f'copy_source: { source_key_excerpt} | copy_key: {copy_keye} ')
    # status_code_excerpt = send_payload(
    #     api_endpoint_url, copy_keye, access_token, mr_name, job_type='auditDrg_excerpts', comments='excerpt inserted')
    # print('excerpt', status_code_excerpt)

    final_js_populate_term, source_key_template = jsDataObj.get_template_selection_population_data(
        claim_id, analysis_json_data, path_to_save_ui_data_result, file_name)
    copy_keytmt = f"{save_path_ui}/{claim_id}/{source_key_template.get('Key').split('/')[-1].replace(claim_id+'_','')}"
    copy_object_to_s3(s3_c, save_path_ui_bucket,source_key_template,copy_keytmt )
    print(f'copy_source: { source_key_template} | copy_key: {copy_keytmt} ')
    # status_code_selection = send_payload(
    #     api_endpoint_url, copy_keytmt, access_token, mr_name, job_type='auditDrg_selections', comments='selection inserted')
    # print('selection', status_code_selection)
    # """
    ################### UI-JSON Result Storing END ###########################

    logging.info(
        f'FUNCTION END: get_json_data.py, saved the json result...')

    logging.info(
        f'FUNCTION END: get_postprocess_data.py, postprocess_table_data:{postprocess_table_data.shape}')
    save_data_loss_log(s3_c, bucket_name,
                       f"{path_to_save_logs}/{data_loss_tracking_file}",
                       f"raw data table postprocess| {postprocess_table_data.shape}", False
                       )
    print(f'final json data stored for {file_name}')
    print(f"data files sent:- {document_name}")

        
    return {
        "statusCode": 200,
        "body": json.dumps({
            "raw data shape": f"{raw_data_df.shape if len(raw_data_df)>0 else (0,0)}",
            "postprocess table data shape": f"{postprocess_table_data.shape if len(postprocess_table_data)>0 else (0,0) }",
            "postprocess excerp data shape": f"{post_process_excerpt_df.shape if len(post_process_excerpt_df)>0 else (0,0)}",
        }),
    }

# if __name__ == "__main__":
#     try: 
#         s3_c = boto3.client('s3', region_name='us-east-1')
#         s3_c.list_buckets()
#         print("S3 client initialized successfully using IAM role in app.py.")
#     except Exception as e:
#         print(f"Failed to initialize S3 client with IAM role: {str(e)} in app.py.")
#         if aws_access_key_id and aws_secret_access_key:
#             s3_c = boto3.client('s3', 
#                                 aws_access_key_id=aws_access_key_id,
#                                 aws_secret_access_key=aws_secret_access_key)
#             print("S3 client initialized successfully using manual keys in app.py.")
#         else:
#             raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in app.py.")

#     try:
#         # cmd=f'python3 {script_path} {bucket_name} {key}'

#         # bucket_name = ""
#         # key = ""
#         bucket_name = sys.argv[1]
#         json_file_path = sys.argv[2]
#         print(sys.argv)

#         response = s3_c.get_object(Bucket=bucket_name,Key=json_file_path)
#         content = response['Body'].read().decode('utf-8')
#         json_data = json.loads(content)

#         print(json_data)
#         bucket_name = json_data['Bucket']
#         key = json_data['Key']
        
#         print(f"Bucket name : {bucket_name}")
#         print(f"S3 Key : {key}")
#         json_data = str({
#             "processing_status":"Started", 
#             "s3_file_path":f"{json_file_path}"
#             })
#         send_sns_message(f"{json_data}")
#         event = {
#           "Records": [
#             {
#               "s3": {
#                 "bucket": {
#                   "name": f"{bucket_name}"
#                 },
#                 "object": {
#                   "key": f"{key}"
#                 }
#               }
#             }
#           ]
#         }
#         response = lambda_handler(event, "")
#         json_data = str({
#             "processing_status":"Completed", 
#             "s3_file_path":f"{json_file_path}"
#             })
#         send_sns_message(f"{json_data}")
#         print("response")
#     except Exception as  e:
#         print(f"EC2 : EXCEPTION OCCURED : {str(e)}")
#         json_data = str({
#             "processing_status":"Error", 
#             "error": f"{str(e)}",
#             "s3_file_path":f"{json_file_path}"
#             })
#         send_sns_message(f"{json_data}")
lambda_handler(event={
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "zai-revmax-qa"
        },
        "object": {
          "key": "devoted/zai_medical_records_pipeline/textract-response/table-json/0abc34ca-5f74-4fa4-aa57-678aa0a0acfd_AJX8E56826_IP1_textract_table_merged.json"
        }
      }
    }
  ]
}
,context='')