import os
import json
import boto3
import logging
import warnings
import urllib.parse
from io import StringIO
from datetime import datetime
warnings.filterwarnings("ignore")

try:
    from utility.get_json_data import JsonData
    from utility.get_index_page import IndexPage
    from constant.constant_dict import mapped_template
    from utility.section_subsection_detection import Section
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from utility.utils import lifecycle_file_generator, send_payload, get_bucket_api, read_csv_file, get_zai_emr_system_name_version, calculate_missing_section_percentage
    from utility.lifecycle_generator import process

except ModuleNotFoundError as e:
    from .utility.get_json_data import JsonData
    from .utility.get_index_page import IndexPage
    from .constant.constant_dict import mapped_template
    from .utility.section_subsection_detection import Section
    from .constant.aws_config import aws_access_key_id, aws_secret_access_key
    from .utility.utils import lifecycle_file_generator, send_payload, get_bucket_api, read_csv_file, get_zai_emr_system_name_version, calculate_missing_section_percentage
    from .utility.lifecycle_generator import process



def lambda_handler(event, context):
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    logging.getLogger('boto').setLevel(logging.ERROR)
    
    # """ auto trigger """
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    textract_file = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_name = textract_file.split('/')[-1]
    document_name = file_name.replace('.csv','.pdf')
    client_name = textract_file.split("/")[0]

    document_directory = file_name.replace('.csv','').replace('_','-').replace('.','-')
    save_path = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/sectioned-data/{document_directory}"

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
    
    claim_id = file_name.split("_")[0]
    client_initial_id = file_name.split("_")[1]
    mr_name = file_name.replace(claim_id+"_", "").replace(".csv", ".pdf")
    # --------------- lambda ---------------
    grd_truth_const_path = f"{client_name}/" + os.environ["GROUND_TRUTH_CONST_PATH"]
    zai_provider_emr_info_params_key = os.environ["ZAI_PROVIDER_EMR_INFO_PARAMS_PATH"]
    zai_sec_subsec_phy_cred_incl_excl_params_key = os.environ["ZAI_SEC_SUBSEC_PHY_CRED_INCL_EXCL_PARAMS_PATH"]
    zai_date_tag_params_key = os.environ["ZAI_DATE_TAG_PARAMS_PATH"]
    zai_audit_process_params_key =  f"{client_name}/" + os.environ["CLAIM_TEMPLATE_MAP_CONSTANT_PATH"] + f"zai_{client_name}_audit_process_params.csv"

    parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]
    destination_bucket, end_point_url_for_lifecycle, save_path_stage, data_dict = get_bucket_api(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key)
    query_name = data_dict['query_name']
    is_lifecycle = data_dict['is_lifecycle']
    zigna_claim_id = data_dict['zigna_claim_id']
    detected_template_ = data_dict['template_name']
    
    # Load the constant files
    # 1. Read the section subsection constant.
    print(f"Reading zai_sec_subsec_phy_cred_incl_excl_params -'{parameters_bucket_name}' - '{zai_sec_subsec_phy_cred_incl_excl_params_key}'")
    section_subsection_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                                Key=zai_sec_subsec_phy_cred_incl_excl_params_key)
    section_subsection_constant_body = section_subsection_constant_object["Body"].read().decode('utf-8')
    section_subsection_constant = json.loads(section_subsection_constant_body)

    # 2. Read the date tag constants.
    print(f"Reading zai_date_tag_params -'{parameters_bucket_name}' - '{zai_date_tag_params_key}'")
    date_tag_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                                Key=zai_date_tag_params_key)
    date_tag_constant_body = date_tag_constant_object["Body"].read().decode('utf-8')
    date_tag_constant = json.loads(date_tag_constant_body)

    #3. Read the ground truth constant file.
    grd_truth_const_file_path = rf"{grd_truth_const_path}{file_name.replace('.csv', '')}_ground_truth_constants.csv"
    print(f"Reading grd_truth_const -'{parameters_bucket_name}' - '{grd_truth_const_file_path}'")
    grd_file = read_csv_file(s3_c, parameters_bucket_name, grd_truth_const_file_path, encoding='latin', dtype = str)

    #4 Read the zai_provider_emr_mapping file
    print(f"Reading zai_provider_emr_info_params -'{parameters_bucket_name}' - '{zai_provider_emr_info_params_key}'")
    zai_provider_emr_mapping = read_csv_file(s3_c, parameters_bucket_name, zai_provider_emr_info_params_key, encoding='latin', dtype = str)

    zai_emr_system_name, zai_emr_system_version = get_zai_emr_system_name_version(grd_file, zai_provider_emr_mapping)
    
    mapped_template = section_subsection_constant['mapped_template']
    if detected_template_ and detected_template_ in mapped_template.keys():
        detected_template = mapped_template.get(detected_template_)
    else:
        detected_template = 'Miscellaneous'

    print(f"detected template: {detected_template}")
    if is_lifecycle == 1:
        process(s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key)
        print('inserted stage file')
    else:
        print(f'processing section-subsection: {file_name}')

        section_object = Section(textract_file,
                                bucket_name = bucket_name,
                                file_name=file_name,
                                document_name = document_name,
                                section_subsection_constant = section_subsection_constant, 
                                date_tag_constant = date_tag_constant, 
                                zai_emr_system_name = zai_emr_system_name, 
                                zai_emr_system_version = zai_emr_system_version,
                                grd_file = grd_file)
        df = section_object.section_subsection_algorithm(save_path, bucket_name)
        calculate_missing_section_percentage(file_name, df, section_object.df_co_date_order_)
        index_obj = IndexPage(file_name, bucket_name, section_subsection_constant)
        index_obj.IndexPage(save_path, section_object.df_co_date_order_, df,section_object.df_co)
        mr_json_data = JsonData.get_chart_view_json_data(
            index_obj.df_chart_view, s3_c, bucket_name, save_path, file_name, parameters_bucket_name, grd_truth_const_path, client_name, mr_name, zai_audit_process_params_key)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "generated section subsection csv",
                "section-subsection shape": f"{df.shape}",
                "physician-info shape": f"{section_object.df_end.shape}",
                "chart-view shape": f"{index_obj.df_chart_view.shape}",
                "date-order shape": f"{index_obj.df_co_date_order.shape}",
                "mr_json_data keys": f"{mr_json_data.keys()}",
            }),
        }
