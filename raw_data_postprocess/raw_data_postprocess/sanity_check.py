import json
import logging
import boto3
import pandas as pd
import urllib.parse
from io import BytesIO, StringIO
import warnings
warnings.filterwarnings("ignore")
try:
    from utility.textract_response import TextractTableResponse
    from utility.textract_response_parsing import TextractTableResponse as TextractTableResponseParsing
    # from utility.table_merger import TableMerger
    from utility.table_merger_v2 import TableMerger
    from utility.table_parser import TableParser
    from utility.get_result import GetResult
    from utility.read_csv_from_s3 import read_csv_from_s3, copy_object_to_s3
    from utility.get_json_data import JsonData
    from utility.get_api import send_payload
    from analysis.pneumonia_analytics import PneumoniaAnalysis
    from analysis.aki_analytics import AKIAnalysis
    from analysis.ami_analytics import AMIAnalysis
    from analysis.generic_analysis import GenericAnalysis

    from constant.postprocess_constant import adm_dis_tag
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from constant.analytics_constant import pneumonia_analytics_range, aki_analytics_range
    from constant.template_excerpt_constant import template_excerpt_data
    from postprocess.post_process import Postprocess
    from postprocess.get_postprocess_data_aki import GetPostProcessData as GetPostProcessDataAKI
    from postprocess.get_postprocess_data_pneumonia import GetPostProcessData as GetPostProcessDataPNM
    from postprocess.get_postprocess_data_generic import GetPostProcessData as GetPostProcessDataGeneric
    from postprocess.get_postprocess_data_ami import GetPostProcessData as GetPostProcessDataAMI
except ModuleNotFoundError as e:
    from .utility.textract_response import TextractTableResponse
    from .utility.textract_response_parsing import TextractTableResponse as TextractTableResponseParsing
    # from .utility.table_merger import TableMerger
    from .utility.table_merger_v2 import TableMerger
    from .utility.table_parser import TableParser
    from .utility.get_result import GetResult
    from .utility.read_csv_from_s3 import read_csv_from_s3, copy_object_to_s3
    from .utility.get_json_data import JsonData
    from .utility.get_api import send_payload
    from .analysis.pneumonia_analytics import PneumoniaAnalysis
    from .analysis.aki_analytics import AKIAnalysis
    from .analysis.ami_analytics import AMIAnalysis
    from .analysis.generic_analysis import GenericAnalysis

    from .constant.postprocess_constant import adm_dis_tag
    from .constant.aws_config import aws_access_key_id, aws_secret_access_key
    from .constant.analytics_constant import pneumonia_analytics_range, aki_analytics_range
    from .constant.template_excerpt_constant import template_excerpt_data
    from .postprocess.post_process import Postprocess
    from .postprocess.get_postprocess_data_aki import GetPostProcessData as GetPostProcessDataAKI
    from .postprocess.get_postprocess_data_pneumonia import GetPostProcessData as GetPostProcessDataPNM
    from .postprocess.get_postprocess_data_generic import GetPostProcessData as GetPostProcessDataGeneric
    from .postprocess.get_postprocess_data_ami import GetPostProcessData as GetPostProcessDataAMI


def lambda_handler(event, context):
    # bucket_name = event['bucket_name']
    # section_subsection_path = event['section_subsection_path']
    # path_to_save_result = event['path_to_save_result']
    # path_to_save_logs = f"{path_to_save_result}/logs"
    # detected_template = event['detected_template']
    """ auto trigger"""

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    table_merged_json = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_name = table_merged_json.split(
        '/')[-1].replace('_textract_table_merged.json', '')
    # adj_tag = file_name.split('_')[0]

    path_to_save_result = f"zai_medical_records_pipeline/medical-records-extract/tables"
    path_to_excerpt_result = f"zai_medical_records_pipeline/medical-records-extract/excerpts"
    path_to_save_logs = f"zai_medical_records_pipeline/medical-records-extract/logs/{file_name}"
    section_subsection_path = f"zai_medical_records_pipeline/medical-records-extract/sectioned-data/{file_name.replace('_','-')}/{file_name.replace('.csv','')}_section_subsection.csv"

    save_path_ui = f"sample_client/audits/medical_records"
    save_path_ui_bucket = 'zai-revmax-test'
    api_endpoint_url = 'https://253w8ijrh0.execute-api.us-east-1.amazonaws.com/Prod/policy/insert_task_into_queue'
    """------------------"""

    claim_id = file_name.split('_')[0]
    print(f'processing: {file_name}| claim id: {claim_id}')
    print('variable loading..')
    # s3_c = boto3.client('s3', aws_access_key_id=aws_access_key_id,
    #                     aws_secret_access_key=aws_secret_access_key)
    # s3_r = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
    #                       aws_secret_access_key=aws_secret_access_key)
    s3_c = boto3.client('s3', region_name='us-east-1')
    s3_r = boto3.resource('s3', region_name='us-east-1')
    # load claim_map file
    # claim_template_object = s3_c.get_object(Bucket=bucket_name,
    #                                         Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/claim_template_map_constant.json')
    # claim_template_body = claim_template_object["Body"].read().decode(
    #     'utf-8')
    # claim_template_map = json.loads(claim_template_body)

    path_for_csv_life_cycle_csv = "mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant"
    csv_life_cycle_obj = s3_c.get_object(
        Bucket='zai-revmax-develop', Key=rf"{path_for_csv_life_cycle_csv}/claim_template_map_constant.csv")
    csv_life_cycle_body = csv_life_cycle_obj['Body']
    csv_life_cycle_string = csv_life_cycle_body.read().decode('utf-8')
    csv_life_cycle = pd.read_csv(StringIO(csv_life_cycle_string))

    csv_life_cycle = csv_life_cycle[csv_life_cycle['is_process'] == 1]
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(
        lambda x: x.replace('.pdf', ''))
    detected_template_ = csv_life_cycle[csv_life_cycle['renamed_medical_record_name']
                                        == file_name.replace('.csv', '')]['template_name'].iloc[0]

    # mapping the detected template
    mapped_template = {'AKI': 'aki', 'ATN': 'aki',
                       'PNA': 'pneumonia', 'MISC': 'generic', 'AMI': 'ami'}
    detected_template = mapped_template.get('MISC')
    # detected_template_ = claim_template_map.get(claim_id)
    if detected_template_:
        detected_template = mapped_template.get(detected_template_)
    else:
        detected_template = 'generic'
    print(f"detected template: {detected_template}")

    # Load the constant files
    template_constant_object = s3_c.get_object(Bucket=bucket_name,
                                               Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/template_constant.json')
    template_constant_body = template_constant_object["Body"].read().decode(
        'utf-8')
    template_constant = json.loads(template_constant_body)

    lab_constant_object = s3_c.get_object(Bucket=bucket_name,
                                          Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/lab_test_constant.json')
    lab_constant_body = lab_constant_object["Body"].read().decode('utf-8')
    lab_constant = json.loads(lab_constant_body)

    # variable loading
    detected_labs = template_constant.get(
        detected_template).get('lab_list')
    template_analytics_dict = template_constant.get(
        detected_template).get('template_analytics_dict')
    suppress_attribute_ls = template_constant.get(
        detected_template).get('suppress_attribute_list_for_flag')
    attribute_name_list = template_constant.get(
        detected_template).get('attribute_dict').keys() - template_constant.get(
        detected_template).get('attribute_regex_dict') - set(suppress_attribute_ls)

    # file_name = section_subsection_path.split(
    #     '/')[-1].replace("section_subsection_", "")
