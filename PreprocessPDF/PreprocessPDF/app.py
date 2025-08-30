import io
import os

import json
import time
import boto3
import botocore
import trp as t1
import urllib.parse
import trp.trp2 as t2
from trp.t_pipeline import add_page_orientation

try:
    from helpers.read_json import read_json
    from helpers.pdfRotation import PDFRotation
    from constants.aws_config import aws_access_key_id, aws_secret_access_key
except ModuleNotFoundError as e:
    from .helpers.read_json import read_json
    from .helpers.pdfRotation import PDFRotation
    from .constants.aws_config import aws_access_key_id, aws_secret_access_key

def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    client = key.split("/")[0]
    file_name = key.split("/")[-1]
    claim_id = file_name.split("_")[0]

    # save_path_bucket = 'zai-revmax-develop'
    # save_path_ui_bucket = 'zai-revmaxai-prod'
    save_path_bucket = bucket_name
    save_path_ui_bucket = os.environ["BUCKET_NAME"]
    if os.environ["PROCESSING_ENVIRONMENT"] == 'qa' or os.environ["PROCESSING_ENVIRONMENT"] == "uat":
        ui_client = "sample_client"
    elif os.environ["PROCESSING_ENVIRONMENT"] == "prod":
        ui_client = client

    highlighted_pdf_doc_path = key
    json_file_name = f'{file_name.replace("_highlighted.pdf","")}_rotation.json'
    json_path_to_save_result = f"{client}/zai_medical_records_pipeline/rotation-test-logs"
    pdf_path_to_save_result = f"{client}/zai_medical_records_pipeline/rotation-test-output"
    path_to_save_result_ui = f"{ui_client}/audits/medical_records/{claim_id}/{file_name.replace(claim_id+'_', '')}"
    textract_json_path = f'{client}/zai_medical_records_pipeline/textract-response/raw-json/{file_name.replace("_highlighted.pdf",  ".json")}'
    
    #s3c initialization
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

    #s3r initialization
    try:
        s3r = boto3.resource('s3', region_name='us-east-1')
        s3r.buckets.all()
        print("S3 resource initialized using IAM role in app.py.")
    except Exception as e:
        if aws_access_key_id and aws_secret_access_key:
            s3r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            print("S3 resource initialized using manual keys in app.py.")
        else:
            raise Exception("S3 resource initialization failed. Check IAM role or AWS credentials in app.py.")

    print("Reading the json file : ", textract_json_path)
    json_dict = read_json(s3c, bucket_name, textract_json_path)
    pdf_obj = PDFRotation(s3c, s3r)
    json_dict = pdf_obj.delete_line_numbers_and_merge_blocks(json_dict)
    angle_dict = pdf_obj.get_page_angle(json_dict, save_path_bucket, json_path_to_save_result, json_file_name)
    pdf_obj.rotate_pdf(file_name, angle_dict, bucket_name, highlighted_pdf_doc_path, pdf_path_to_save_result, save_path_ui_bucket, path_to_save_result_ui)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }