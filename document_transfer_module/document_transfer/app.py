import os
import re
import json
import boto3
import urllib.parse
import pandas as pd
import builtins
from datetime import datetime
from dotenv import load_dotenv

try:
    from helpers.custom_logger import enable_custom_logging
    from helpers.get_cateogery import GetCateogery
    from helpers.duplicate_check import DupicateCheck
    from helpers.arl_check import UniqueClaimIdentifierCheck as ARLCheck
    from helpers.constant import copy_object, delete_object, read_json_from_s3
    from helpers.textract import *
    from helpers.get_data_dict import get_data_dict
    from constants.aws_config import aws_access_key_id, aws_secret_access_key
    from helpers.received_file_logs import ReceivedFileLogs
    from helpers.chunk_pdf import PDF
    from helpers.manifest import UpdateManifest
    from helpers.generate_validation_query import generate_query, get_all_pcn_and_arl

except ModuleNotFoundError as e:
    from .helpers.custom_logger import enable_custom_logging
    from .helpers.get_cateogery import GetCateogery
    from .helpers.duplicate_check import DupicateCheck
    from .helpers.arl_check import UniqueClaimIdentifierCheck as ARLCheck
    from .helpers.constant import copy_object, delete_object, read_json_from_s3
    from .helpers.textract import *
    from .helpers.get_data_dict import get_data_dict
    from .constants.aws_config import aws_access_key_id, aws_secret_access_key
    from .helpers.received_file_logs import ReceivedFileLogs
    from .helpers.chunk_pdf import PDF
    from .helpers.manifest import UpdateManifest
    from .helpers.generate_validation_query import generate_query, get_all_pcn_and_arl


enable_custom_logging()

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']   
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    client_name = key.split("/")[0]
    document_name = key.split("/")[-1]
    document_type = document_name.split(".")[-1]
    print(rf"Received file:- {document_name}")

    try:
        s3c = boto3.client('s3', region_name='us-east-1')
        s3c.list_buckets()
        print("S3 client initialized successfully using IAM role in app.py.")
    except Exception as e:
        print(f"Failed to initialize S3 client with IAM role: {str(e)} in app.py.")
        if aws_access_key_id and aws_secret_access_key:
            s3c = boto3.client('s3', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
            print("S3 client initialized successfully using manual keys in app.py.")
        else:
            raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in app.py.")
        
    try:
        s3r = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        s3r.buckets.all()
        print("S3 resource initialized using IAM role in app.py.")
    except Exception as e:
        if aws_access_key_id and aws_secret_access_key:
            s3r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            print("S3 resource initialized using manual keys in app.py.")
        else:
            raise Exception("S3 resource initialization failed. Check IAM role or AWS credentials in app.py.")
    
    if aws_access_key_id == ''  and aws_secret_access_key == '':
        athena_client = boto3.client('athena', region_name='us-east-1')
        print("Athena service initialized using IAM role in app.py.")
    else:
        athena_client = boto3.client('athena', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print("Athena service initialized using manual keys in app.py.")

    response = s3c.head_object(Bucket=bucket_name, Key=key)

    document_size = response['ResponseMetadata']['HTTPHeaders']['content-length']

    load_dotenv()

    parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]
    zai_date_tag_params_key = os.environ["ZAI_DATE_TAG_PARAMS_PATH"]
    document_pattern_json_key = os.environ["DOCUMENT_TRANSFER_PARAMS_PATH"]
    common_prefix = f"{client_name}/audits/sftp/"
    medical_record_path = f'{client_name}/audits/sftp/medical_records/'
    
    document_manual_review_path = rf"{client_name}/audits/sftp/manual_review/"
    medical_record_manual_review_path = rf"{client_name}/audits/sftp/medical_records/manual_review/"
    
    duplicate_document_path = rf"{client_name}/audits/sftp/duplicate/"
    duplicate_medical_record_path = rf"{client_name}/audits/sftp/medical_records/duplicate/"
    
    received_file_logs_path = f"{client_name}/audits/sftp/received_file_logs/" 
    received_file_logs_key = f"{client_name}/audits/sftp/received_file_logs/received_files_logs_2.txt"
    document_extension = ['zip', 'txt', 'gz', 'pdf', 'tiff', 'csv', 'xlsx', 'filepart']
    compressed_file_extension = ['zip', 'gz', 'filepart']
    
    batch_size = 550
    STATIC_COLS = ['Vendor Code', 'Vendor Audit ID', 'Client Audit ID','Adjudication Record Locator', 'Member Record Locator','Document Type Code', 'PDF_Name', 'Document Rec Date', 'Action Date','zigna_s3_path', 'zip_file_name', 'total_files_received']
    MANIFEST_FOLDER = f'{client_name}/audits/sftp/medical_records/manifest'
    MANIFEST_FOLDER_CLEANED = f'{client_name}/audits/sftp/medical_records/file_manifest'
    TEMPORARY_COMPRESSED_FILE_FOLDER = f'{client_name}/audits/sftp/temporary_compressed_files'

    print(f"Client : {client_name}")
    print(f"Reading document_transfer_params - '{parameters_bucket_name}' - '{document_pattern_json_key}'")
    client_doc_pattern = read_json_from_s3(s3c, parameters_bucket_name, document_pattern_json_key)
    client_doc_pattern_json = client_doc_pattern['clients'][client_name]
    vendor_code = client_doc_pattern_json['vendor_code']
    
    print(f"Reading zai_date_tag_params_key - '{parameters_bucket_name}' - '{zai_date_tag_params_key}'")
    date_tag_constants = read_json_from_s3(s3c, parameters_bucket_name, zai_date_tag_params_key)

    if document_type.lower() not in document_extension:
        document_type = None
        print(f"Document extension not found for '{key}'")
    
    if document_type and document_type not in compressed_file_extension:
        get_cateogery_obj =  GetCateogery(s3c, s3r, bucket_name, key, \
                                                client_name, document_name, common_prefix)
        received_file_logs_obj = ReceivedFileLogs(s3r, client_name, bucket_name, received_file_logs_path, received_file_logs_key)
        doc_obj =  DupicateCheck(s3c, s3r, bucket_name, key, client_name, document_name, document_type, response, \
                                                            received_file_logs_key, received_file_logs_path, received_file_logs_obj, document_manual_review_path,\
                                                            medical_record_manual_review_path, duplicate_document_path, duplicate_medical_record_path)
        medical_record_pattern = [i for i in client_doc_pattern_json['get_document_info'] if i['document_name'] == 'medical_record'][0]['pattern']
        adjudication_record_locator = re.findall(medical_record_pattern, document_name.upper())[0].upper() if len(re.findall(medical_record_pattern, document_name.upper())) > 0 else None
        if document_type == 'pdf':

            print(rf"query for all related ARLs started:- {document_name}")
            df_arl = get_all_pcn_and_arl(athena_client, s3c, adjudication_record_locator, client_doc_pattern_json)
            print(rf"query for all related ARLs completed:- {document_name}")
            try:
                root_payer_control_number = df_arl['root_payer_control_number'].iloc[0]
            except:
                root_payer_control_number=''
        else:
            df_arl = pd.DataFrame({})
            root_payer_control_number = ''
        print(rf"duplication check started:- {document_name}")
        is_document_duplicated , document_type_code, \
            document_storage_path_, document_name_cleaned = doc_obj.duplicate_check(client_doc_pattern_json, get_cateogery_obj, document_type, df_arl)
        print(rf"duplication check completed:- {document_name}")
        
        print("Document storage path after duplcicate check : ", document_storage_path_)

        dest_key ,is_arl_present_in_document_name, \
                is_arl_received, is_arl_matched_with_document, is_document_sent_for_manual_review = None, None, None, None, None
        

        if document_type_code == 'medical_record':
            if (not is_document_duplicated):
                print(rf"text extraction started:- {document_name_cleaned}")
                json_path = extract_text_from_pdf(bucket_name, key, medical_record_path, False)
                print(rf"text extraction completed:- {document_name_cleaned}")

                arlObj = ARLCheck(s3c, athena_client, client_name, document_name_cleaned,client_doc_pattern_json, date_tag_constants, adjudication_record_locator, root_payer_control_number)

                print(f"PHI validation started:- {document_name_cleaned}")
                dest_key ,is_arl_present_in_document_name, is_arl_received, \
                    is_arl_matched_with_document, is_document_sent_for_manual_review, validation_query_df = arlObj.get_unique_claim_identifier_status(bucket_name, document_storage_path_, json_path,document_name_cleaned)
                print(f"PHI validation completed:- {document_name_cleaned}")
                
                # If we get the dest_key same as key(i.e. document key), i.e. the ARL is matched we can go with the document_storage_path_ else ARL mismatch (i.e. moved to Manual Review)
                document_storage_path = document_storage_path_ if key == dest_key else dest_key
                
                print(f'copying file to raw folder:- {document_name_cleaned}')
                copy_object(s3c, bucket_name, key, bucket_name, document_storage_path)
                print(f'copied file to raw folder:- {document_name_cleaned}')

                print(f"Doing the validation check for {document_name}")
                pdf_obj = PDF(s3c, s3r,document_name_cleaned, medical_record_path, client_name, batch_size)
                zip_file_count , manifest_files , zip_folder_name, \
                    is_zigna_manifest, pdf_name_map= pdf_obj.pdf_manifest(bucket_name, key, document_storage_path, validation_query_df, vendor_code, adjudication_record_locator)
                if zip_file_count == 0:
                    return document_storage_path
                manifest_obj = UpdateManifest(s3c, s3r, bucket_name,document_name_cleaned, pdf_obj, validation_query_df, STATIC_COLS, MANIFEST_FOLDER, MANIFEST_FOLDER_CLEANED, client_doc_pattern_json, adjudication_record_locator) 
                manifest_obj.update_manifest(manifest_files, pdf_name_map, document_name_cleaned, \
                                             zip_folder_name, is_zigna_manifest, is_arl_present_in_document_name, is_arl_received, is_arl_matched_with_document)


                # data = (document_storage_path, document_name_cleaned, document_size, document_type_code,\
                #     is_arl_present_in_document_name, is_arl_matched_with_document, is_document_sent_for_manual_review, is_document_duplicated)
                # data_dict = get_data_dict(data)
            else:
                print(f'The Document {document_name} is duplicated')
                copy_object(s3c, bucket_name, key, bucket_name, document_storage_path_)
        else:
            copy_object(s3c, bucket_name, key, bucket_name, document_storage_path_)
    else:
        if document_type in compressed_file_extension:
            print(f"It is a '{document_type}' document, moving it to temporary location")
            document_storage_path_ = f"{TEMPORARY_COMPRESSED_FILE_FOLDER}/{document_name}"
            copy_object(s3c, bucket_name, key, bucket_name, document_storage_path_)
        else:
            print("The document extension is not listed/found in the pre-defined extensions")
            document_storage_path_ = f"{document_manual_review_path}/{document_name}"
            copy_object(s3c, bucket_name, key, bucket_name, document_storage_path_)
    
        return {
                "statusCode": 200,
                "body": json.dumps({
                        "message": "hello world",
                        # "location": ip.text.replace("\n", "")
                }),
        }


keys=[

'H00011293296IP2_04-03-2025_15-02.pdf',
'H00011293296IP2_04-03-2025_15-28.pdf',
'H00011293296IP2_04-11-2025_12-36.pdf',
'2224295IP1_04-09-2025_15-04.pdf',
'2226238-IP1.pdf',
'2227035-IP1.pdf',
'2217709IP1_04-09-2025_14-46.pdf',
'2223624IP1_04-09-2025_14-52.pdf',
'MRH_Zigna_ClaimLines_041125.csv',
'V00011737122IP61_08-29-2025_05-31.pdf',
'V00011737566IP61_08-28-2025_11-48.pdf',
'V00011739767IP61_08-27-2025_16-48.pdf',
'MRH_Zigna_Claims_041125.csv',
'MRH_Zigna_Patient_041125.csv',
'MRH_Zigna_Proivder_041125.csv',
]
for key in keys:
    
    event = {
    "Records": [
    {
        "s3": {
        "bucket": {
            "name": "zai-revmax-qa"
        },
        "object": {                                                                           
            "key": f"helix/audits/sftp/inboundFromClient/{key}"
        }
        }
    }
    ]
    }
    lambda_handler(event, '')