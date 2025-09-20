import json
import time
import boto3
import PyPDF2
from io import StringIO
from io import BytesIO,BufferedReader
try:
    from constants.aws_config import aws_access_key_id, aws_secret_access_key
    from helpers.constant import * 
except:
    from ..constants.aws_config import aws_access_key_id, aws_secret_access_key
    from ..helpers.constant import * 

try:   
    s3c = boto3.client('s3', region_name='us-east-1')
    s3c.list_buckets()
    print("S3 client initialized successfully using IAM role in textract.py.")
except Exception as e:
    print(f"Failed to initialize S3 client with IAM role: {str(e)} in textract.py.")
    if aws_access_key_id and aws_secret_access_key:
        s3c = boto3.client('s3', 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
        print("S3 client initialized successfully using manual keys in textract.py.")
    else:
        raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in textract.py.")
try:
    s3r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    s3r.buckets.all()
    print("S3 resource initialized using IAM role in textract.py.")
except Exception as e:
    if aws_access_key_id and aws_secret_access_key:
        s3r = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print("S3 resource initialized using manual keys in textract.py.")
    else:
        raise Exception("S3 resource initialization failed. Check IAM role or AWS credentials in aptextractp.py.")

if aws_access_key_id != '' and aws_secret_access_key != '':
    textract = boto3.client('textract',aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    print("Textract service initialized successfully using manual keys in textract.py.")
else:
    textract = boto3.client('textract',region_name='us-east-1')
    print("Textract service initialized successfully using IAM role in textract.py.")
# MEDICAL_RECORD_PATH = 'devoted/audits/sftp/medical_records/'

def start_document_text_detection(s3_bucket_name, document_key,is_table = False):

    if is_table:
        response = textract.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': s3_bucket_name,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES']
        )
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': document_key
            }
        }
    )

    return response['JobId']

def is_job_complete(job_id):
    while True:
        response = textract.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']
        if status in ['SUCCEEDED', 'FAILED']:
            return status
        print(f'Job status: {status}, waiting for completion...')
        time.sleep(5)

def get_job_results(job_id):
    pages = []
    response = textract.get_document_text_detection(JobId=job_id)
    pages.append(response)
    next_token = response.get('NextToken')

    while next_token:
        response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        next_token = response.get('NextToken')

    return pages

def getJobResultForText(jobId):
    pages = []

    response = textract.get_document_text_detection(JobId=jobId)
    pages.append(response)

    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']

    while (nextToken):

        response = textract.get_document_text_detection(
            JobId=jobId, NextToken=nextToken)
        pages.append(response)
        nextToken = None

        if ('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def getJobResultForTable(jobId):
    maxResults = 1000
    paginationToken = None
    finished = False
    pages = []
    while finished == False:
        response = None
        if paginationToken == None:
            response = textract.get_document_analysis(
                JobId=jobId, MaxResults=maxResults)
        else:
            response = textract.get_document_analysis(
                JobId=jobId, MaxResults=maxResults, NextToken=paginationToken)
        pages.append(response)
        print('Document Detected.')
        if 'NextToken' in response:
            paginationToken = response['NextToken']
        else:
            finished = True

    return pages



def infer_job(job_id,is_table = True):

    response = None
    status = is_job_complete(job_id)
    if status == 'SUCCEEDED':
        print('Job status: SUCCEEDED, waiting for completion...')
        if is_table:
            response = getJobResultForTable(job_id)
        else:
            response = getJobResultForText(job_id)
    else:
        print(f'Textract job failed for {job_id}')

    return response

def add_line_number(response):

    raw_json = None
    count = 1
    n = 1
    if response:
        for ele in response:
            for blk in  ele.get('Blocks')[1:]:
                if blk.get('Page') == count +1:
                    count = count +1
                    n=1
                elif blk.get('BlockType') == 'LINE':
                    blk.update({'LINE_NUMBER':f'{n}'})
                    n+=1
        raw_json = json.dumps(response)
    else:
        print('No response returned')
    return raw_json

def store_response(line_added_response, s3_bucket_name, document_key, medical_record_path, is_table):
    key = None
    if is_table:
        output_json_file_name = document_key.split('/')[-1].replace('.pdf', '_table.json')
        key = medical_record_path+f'textract-response/table-raw-json/{output_json_file_name}'
        s3c.put_object(Body=line_added_response, Bucket=s3_bucket_name,
                      Key=key)
    else:
        output_json_file_name = document_key.split('/')[-1].replace('.pdf', '.json')
        key = medical_record_path+f'textract-response/raw-json/{output_json_file_name}'
        s3c.put_object(Body=line_added_response, Bucket=s3_bucket_name,
                      Key=key)
    return key

def get_max_n_pages(bucket_name,new_folder,key,n=10):
    # print("Entered get max n pages")
    print(f"Extracting {n} pages from textract")
    obj = s3r.Object(bucket_name,key)
    fs = obj.get()['Body'].read()
    pdfFile = PyPDF2.PdfReader(BufferedReader(BytesIO(fs)))
    no_of_pages = len(pdfFile.pages)
    # print("no of pages",no_of_pages)
    pdfwriter = PyPDF2.PdfWriter()
    for index in range(min(no_of_pages,n)):
        # print("page: ", index)
        page = pdfFile.pages[index]
        pdfwriter.add_page(page)
    with BytesIO() as bytes_stream:
        pdfwriter.write(bytes_stream)
        filename = key.split('/')[-1]
        filename = filename.replace('.pdf','')
        tmp_file_name = f'{new_folder}/{filename}_tmp.pdf'
        s3r.Object(bucket_name,tmp_file_name).put(Body=bytes_stream.getvalue(),ContentType='application/pdf')
    return tmp_file_name

def extract_text_from_pdf(s3_bucket_name, document_key, medical_record_path, is_table = False):
    '''
    ### Extracts the pdf text and stores the json file s3 location
    
    args:
        s3_bucket_name: string (Name of the S3 bucket)
        document_key: string (file path)
    return:
        out_path: string (output path)
    '''
    print(f'Extract text from pdf started for {s3_bucket_name} - {document_key} and table extraction is {is_table}')
    new_folder = medical_record_path+'temp_pdfs'
    textract_pdf_key = get_max_n_pages(s3_bucket_name,new_folder,document_key)
    job_id = start_document_text_detection(s3_bucket_name, textract_pdf_key,is_table)
    print(f'Job ID {job_id} generated for {s3_bucket_name} - {textract_pdf_key} ')
    response = infer_job(job_id,is_table)
    out_path = ''
    if response:
        print(f'Response generated for {job_id} with length of {len(response)} generated for {s3_bucket_name} - {textract_pdf_key} ')
        line_added_response = add_line_number(response)
        print(f'Line number generated for {job_id} - {s3_bucket_name} - {document_key}')
        out_path = store_response(line_added_response, s3_bucket_name, textract_pdf_key, medical_record_path, is_table)
        print(f'Response {job_id} - {s3_bucket_name} - {textract_pdf_key} stored for in location {out_path}')
    else:
        print(f'Response not generated for {job_id} for {s3_bucket_name} - {document_key} ')
        print(f"text extraction failed:- {document_key.split('/')[-1]}")
    delete_object(s3c, s3_bucket_name,textract_pdf_key)
    return out_path