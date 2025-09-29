from vital_extraction.vital_excerpts import VitalExcerpt
from utility.excerpt_extraction import ExcerptExtraction
from utility.pdf_merger import PdfMerger
from utility.pdf_highlight import HighlightPdf
from utility.copy_source_s3 import copy_object_to_s3
from constant.date_tag_constant import date_tags, suppress_date_tag
from constant.aws_config import aws_access_key_id, aws_secret_access_key

# from constant.template_excerpt_constant import template_excerpt_data
import os
import json
import logging
import warnings
import boto3
import urllib.parse
from io import BytesIO, StringIO
import pandas as pd
warnings.filterwarnings("ignore")
# from constant.vital_constant import *

from get_lab_extraction import get_lab_metadata
from get_bucket_api import get_bucket_api
import fitz
import sys
from helpers.custom_logger import S3Logger

logger = S3Logger()

def lambda_handler(event, context):
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    logging.getLogger('boto').setLevel(logging.ERROR)
    if os.getenv("AWS_EXECUTION_ENV") is None:
        from dotenv import load_dotenv
        load_dotenv()
        print("Getting the environment variables form .env")
    
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

    # print(fitz.__version__)
    """ auto trigger"""
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    sec_subsec_csv = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    client_name = sec_subsec_csv.split("/")[0]
    file_name = sec_subsec_csv.split('/')[-1].replace('_section_subsection', '')
    document_name=file_name.replace('.csv','.pdf')
    print(f"started vitalLabExtraction processing:- {document_name}")
    path_to_save_result = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/excerpts" 
    path_to_save_logs = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/logs/{file_name.replace('.csv','')}"
    textract_csv_path = f"{client_name}/zai_medical_records_pipeline/textract-response/json-csv/{file_name}"
    pdf_doc_path = f"{client_name}/zai_medical_records_pipeline/concat-files/digitized-pdf/{file_name.replace('.csv','.pdf')}"
    merged_pdf_save_path = f"{client_name}/zai_medical_records_pipeline/concat-files/merged-pdf"
    highlight_pdf_save_path = f"{client_name}/zai_medical_records_pipeline/medical-records-extract/pdf-highlight"
    ground_truth_const = f"{client_name}/" + os.environ["GROUND_TRUTH_CONST_PATH"]
    zai_audit_process_params_key =  f"{client_name}/" + os.environ["CLAIM_TEMPLATE_MAP_CONSTANT_PATH"] + f"zai_{client_name}_audit_process_params.csv"
    excerpts_regex_lab_list_disease_detection_params_key = os.environ["EXCERPTS_REGEX_LAB_LIST_DISEASE_DETECTION_PARAMS_PATH"]
    zai_lab_header_range_key = os.environ["ZAI_LAB_HEADER_RANGE_PATH"]
    zai_distinct_section_params_key = os.environ["ZAI_DISTINCT_SECTION_PARAMS_PATH"]
    parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]

    ground_truth_const = f'{client_name}/generated_logs/ground_truth_constants/'

    # 'claimId_mrName_ground_truth_constants.csv'
    """------------------"""
    
    claim_id = file_name.split('_')[0]
    print(f'processing: {file_name}| claim id: {claim_id}')
    print(f"s3_key : {sec_subsec_csv}")
    print('loading constant...')
    ### template detection and respective variable loading
    logging.info('VARIABLE LOADING: template attribute loading...')
    # attribute_dict = template_excerpt_data.get(
    #     detected_template).get('attribute_dict')
    # attribute_regex_dict = template_excerpt_data.get(
    #     detected_template).get('attribute_regex_dict')
    # lab_extraction_key = template_excerpt_data.get(
    #     detected_template).get('lab_list')
    
    # load claim_map file/ docc
    # claim_template_object = s3_c.get_object(Bucket=bucket_name,
    #                                            Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/claim_template_map_constant.json')
    # claim_template_body = claim_template_object["Body"].read().decode(
    #     'utf-8')
    # claim_template_map = json.loads(claim_template_body)
    #----grd-truth----
    ground_truth_const_ = f'{ground_truth_const}{file_name.replace(".csv","")}_ground_truth_constants.csv'
    # s3_c = boto3.client('s3', aws_access_key_id=aws_access_key_id,
    #                     aws_secret_access_key=aws_secret_access_key)
    bucket_name_constant = bucket_name
    # """
    # UI data
    highlight_pdf_save_path_ui_bucket, _, highlight_pdf_save_path_ui = get_bucket_api(
        s3_c, file_name, parameters_bucket_name, client_name, zai_audit_process_params_key)
    # """
    print(f"Reading ground_truth_const -'{parameters_bucket_name}' - '{ground_truth_const_}'")
    grd_obj = s3_c.get_object(
        Bucket=parameters_bucket_name, Key=ground_truth_const_)
    grd_body = grd_obj['Body']
    grd_body_string = grd_body.read().decode('utf-8')
    grd_df = pd.read_csv(StringIO(grd_body_string))
    adm_date = grd_df['admission_date'].iloc[0]
    disch_date = grd_df['discharge_date'].iloc[0]

    csv_life_cycle_obj = s3_c.get_object(
        Bucket=bucket_name_constant, Key=zai_audit_process_params_key)
    csv_life_cycle_body = csv_life_cycle_obj['Body']
    csv_life_cycle_string = csv_life_cycle_body.read().decode('utf-8')
    csv_life_cycle = pd.read_csv(StringIO(csv_life_cycle_string), encoding='latin', dtype=str)
    
    csv_life_cycle['is_process'] = csv_life_cycle['is_process'].astype('int32')
    csv_life_cycle = csv_life_cycle[csv_life_cycle['is_process'] == 1] #uncomment it
    csv_life_cycle['renamed_medical_record_name'] = csv_life_cycle['renamed_medical_record_name'].apply(
        lambda x: x.replace('.pdf', ''))
    detected_template_ = csv_life_cycle[csv_life_cycle['renamed_medical_record_name']
                                        == file_name.replace('.csv', '')]['template_name'].iloc[0]

    # mapping the detected template
    mapped_template = {'AKI': 'aki', 'ATN': 'aki', 'PNA': 'pneumonia', 'MISC': 'generic', 'AMI': 'ami', 'SEPSIS': 'sepsis', "ENCEPHALOPATHY" : "Encephalopathy"}
    
    detected_template = mapped_template.get('MISC')
    # detected_template_ = claim_template_map.get(claim_id)
    # if detected_template_ and detected_template_ in mapped_template.keys():
    #     detected_template = mapped_template.get(detected_template_)
    # else:
    #     detected_template = 'generic'
    print(f"detected template: {detected_template}")

    # Load the constant files
    print(f"Reading excerpts_regex_lab_list_disease_detection_params -'{parameters_bucket_name}' - '{excerpts_regex_lab_list_disease_detection_params_key}'")
    template_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                               Key=excerpts_regex_lab_list_disease_detection_params_key)
    template_constant_body = template_constant_object["Body"].read().decode('utf-8')
    template_constant = json.loads(template_constant_body)

    print(f"Reading zai_lab_header_range - '{parameters_bucket_name}' - '{zai_lab_header_range_key}'")
    lab_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                          Key=zai_lab_header_range_key)
    lab_constant_body = lab_constant_object["Body"].read().decode('utf-8')
    lab_constant = json.loads(lab_constant_body)

    print(f"Reading zai_distinct_section_params - '{parameters_bucket_name}' - '{zai_distinct_section_params_key}'")
    sections_constant_object = s3_c.get_object(Bucket=parameters_bucket_name,
                                          Key=zai_distinct_section_params_key)
    sections_constant_body = sections_constant_object["Body"].read().decode('utf-8')
    sections_constant = pd.read_csv(StringIO(sections_constant_body))

    lab_extraction_key = template_constant.get(
        detected_template).get('lab_list')
    attribute_dict = template_constant.get(
        detected_template).get('attribute_dict')
    attribute_regex_dict = template_constant.get(
        detected_template).get('attribute_regex_dict')
    logging.info('VARIABLE LOADING: template attribute loading done!')
    
    min_max_date = (str(adm_date), str(disch_date))
    print(min_max_date)

    ######### vital excerpt ##########
    print('vital excerpt...')
    logging.info('FUNCTION START: vital_excerpt.py')
    print(f"vitals extraction by regex started:- {document_name}")
    vital_excp_object = VitalExcerpt(bucket_name,
                                     sec_subsec_csv, date_tags, suppress_date_tag, lab_constant.get('Vital').get('lab_regex_list'),
                                     min_max_date, textract_csv_path)
    vital_metadata = vital_excp_object.get_vitals()
    vital_excerpt_df = vital_excp_object.save_vital_result(
        vital_metadata, path_to_save_result)
    print(f"vitals extraction by regex completed:- {document_name}")
    logging.info(
        f'FUNCTION END: vital excerpts detected, saved the result in {path_to_save_result}')
    
    page_list = []
    print('lab page detection...')
    print(f"labpage detection started:- {document_name}")

    lab_extraction_data = get_lab_metadata(bucket_name,
                                           sec_subsec_csv, file_name, path_to_save_logs, lab_extraction_key, lab_constant, sections_constant)
    
    for lab_test, mdata in lab_extraction_data.items():
        page_list += mdata.get('page_list')
    print(f"labpage detection completed:- {document_name}")
    ####### excerpt extraction #######
    print('excerpt extraction...')
    print(f"excerpt extraction started:- {document_name}")
    logging.info('FUNCTION START: excerpt_extraction.py')
    excerpt_obj = ExcerptExtraction(
        sec_subsec_csv, textract_csv_path, bucket_name,
        date_tags,
        suppress_date_tag,min_max_date,
        f'{detected_template}',
        attribute_dict, attribute_regex_dict)

    excerpt_result = excerpt_obj.get_excerpts()
    excerpt_result_df = excerpt_obj.get_result_df(excerpt_result)
    excp_present = False
    if excerpt_result_df.shape[0] >= 1:
        excerpt_result_df = excerpt_obj.check_exclusion(excerpt_result_df)

    # if str(type(excerpt_result_df)) == "<class 'pandas.core.frame.DataFrame'>":
    if excerpt_result_df.shape[0] >= 1:
        excerpt_obj.save_excerpt_result(
            excerpt_result_df, s3_c, bucket_name, path_to_save_result)
        excp_present = True
        logging.info(
            f'FUNCTION END: excerpt extracted.. saved the result in {path_to_save_result}')
    print('excerpt present: ', excp_present)
    print(f"excerpt extraction completed:- {document_name}")

    

    print('pdf merging...')
    print(f"merging labs started:- {document_name}")
    logging.info('FUNCTION START: pdf_merger.py')
    PdfMerger.merged_pdf(s3_r, bucket_name, merged_pdf_save_path,
                         page_list, pdf_doc_path)
    logging.info(
        f'FUNCTION END: PDF merged! saved result in {merged_pdf_save_path}')
    print(f"merging labs completed:- {document_name}")
    
    if detected_template_ and detected_template_ in mapped_template.keys():
        detected_template = mapped_template.get(detected_template_)
    else:
        detected_template = 'generic'

    attribute_dict = template_constant.get(
        detected_template).get('attribute_dict')
    attribute_regex_dict = template_constant.get(
        detected_template).get('attribute_regex_dict')
    excerpt_attribute_ls = list(attribute_dict.keys()) + list(attribute_regex_dict.keys())

    if excerpt_result_df.shape[0] > 0:
        excerpt_result_df = excerpt_result_df[excerpt_result_df['TestName'].isin(excerpt_attribute_ls)]
    if 'index' in excerpt_result_df.columns:
        excerpt_result_df.drop(columns=['index'], inplace=True)
    excerpt_result_df = excerpt_result_df.reset_index()

    ##### Highlighting PDF #####
    print(f"highlighting started:- {document_name}")
    print('pdf highlight...')
    logging.info('FUNCTION START: pdf_highlight.py')
    if excp_present and excerpt_result_df.shape[0] > 0:
        # pdf_doc = fitz.open(pdf_doc_path)
        pdf_obj = s3_r.Object(
                bucket_name, pdf_doc_path)
        pdf_obj_body = pdf_obj.get()["Body"].read()
        pdf_doc = fitz.open("pdf", stream=BytesIO(pdf_obj_body))
        copy_source = HighlightPdf(pdf_doc).highlight_pdf(excerpt_result_df,
                                            pdf_doc, pdf_doc_path.split(
                                                "/")[-1],
                                            highlight_pdf_save_path, s3_r, bucket_name)
        print(f"highlighting completed:- {document_name}")
    
    # """
        # UI data
        if copy_source:
            # copy_key = f"{highlight_pdf_save_path_ui}/{claim_id}/{copy_source.get('Key').split('/')[-1]}" with claim_id
            copy_key = f"{highlight_pdf_save_path_ui}/{claim_id}/{copy_source.get('Key').split('/')[-1].replace(claim_id+'_','')}"
            copy_object_to_s3(s3_c, highlight_pdf_save_path_ui_bucket,
                              copy_source, copy_key, content_type='application/pdf')
            print(f'copy_source: { copy_source} | copy_key: {copy_key} ')
        logging.info(
            f'FUNCTION END: PDF doc highlighted...Saved the result in {highlight_pdf_save_path}')
    else:
        print('not found excerpt...saving the pdf!')
        copy_source = {'Bucket': bucket_name,
                    #    'Key': f'{highlight_pdf_save_path}/{pdf_doc_path.split("/")[-1].replace(".pdf","")}_highlighted.pdf'}
                       'Key': pdf_doc_path}
        copy_key = f"{highlight_pdf_save_path_ui}/{claim_id}/{copy_source.get('Key').split('/')[-1].replace(claim_id+'_','').replace('.pdf','')}_highlighted.pdf"
        copy_object_to_s3(s3_c, highlight_pdf_save_path_ui_bucket,
                          copy_source, copy_key, content_type='application/pdf')
        print(f'copy_source: { copy_source} | copy_key: {copy_key} ')
        logging.info(
            f'FUNCTION END: not found excerpt...')
    # """
    # put the empty json if the len() page list is 0
    if not page_list:
        s3_c.put_object(Bucket=bucket_name, Body=json.dumps({'TablePresent': 0}),
                        Key=f'{client_name}/zai_medical_records_pipeline/textract-response/table-json/{file_name.replace(".csv","_textract_table_merged.json")}')
         
    print(f"completed vitalLabExtraction processing:- {document_name}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "lab extraction",
            "vital excerpt df shape": f"{vital_excerpt_df.shape if len(vital_excerpt_df)>0 else (0,0)} ",
            "lab_pages": f"{list(set(page_list))}",
            "detected_excerpt": f"{excerpt_result_df['TestName'].value_counts().to_dict() if excerpt_result_df.shape[0] > 0 else {}}"
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
#     sns_topic_arn = "arn:aws:sns:us-east-1:833984991867:revmaxai_mr_data_processing_alarms"
#     sns_client = boto3.client("sns", region_name = "us-east-1")
#     def send_sns_message(messaage):
#         response = sns_client.publish(
#             TopicArn = sns_topic_arn,
#             Message = messaage,
#             Subject = "EC2 Notification"
#         )
#         print(f"Message sent! Message ID: {response['MessageId']}")

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
#         # s3_file_path = ""
#         json_data = str({
#             "processing_status":"Completed", 
#             "s3_file_path":f"{json_file_path}"
#             })
#         send_sns_message(f"{json_data}")

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
          "key": ""
        }
      }
    }
  ]
},
context='')