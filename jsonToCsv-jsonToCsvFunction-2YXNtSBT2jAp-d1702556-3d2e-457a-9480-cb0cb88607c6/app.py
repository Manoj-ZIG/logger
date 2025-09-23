import json
import pandas as pd
from io import StringIO
import boto3
from urllib.parse import unquote_plus
from python_to_java_json_conversion import convert_format
from constants.aws_config import aws_access_key_id,aws_secret_access_key
from helpers.custom_logger import enable_custom_logging
enable_custom_logging()




s3 = boto3.client('s3', region_name='us-east-1',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

def move_pdfs_for_digitization(bucket_name, file_name):

    print(f'Searching for raw input pdf in concat-files/raw-input-pdfs')
    input_raw_pdf_path = file_name.replace("textract-response/raw-json","concat-files/raw-input-pdfs").replace(".json",".pdf")
    copy_source = {'Bucket': bucket_name,'Key': input_raw_pdf_path}
    input_digitize_pdf_path = input_raw_pdf_path.replace('raw-input-pdfs','digitize-input-pdfs')
    response = s3.copy_object(CopySource=copy_source, Bucket= bucket_name,
                        Key=input_digitize_pdf_path)
    print('Moved raw input pdf to digitize-input-pdfs Successfully')

def lambda_handler(event, context):

    file_obj = event["Records"][0]
    bucket_name = str(file_obj["s3"]["bucket"]["name"])
    file_name = unquote_plus(str(file_obj["s3"]["object"]["key"]))
    document_name = file_name.split('/')[-1].replace('.json','.pdf')

    print(file_name, bucket_name, 1)

    print(f"reading the raw json file:- {document_name}")
    s3_clientobj = s3.get_object(Bucket=bucket_name, Key=file_name)
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')

    jsn = json.loads(s3_clientdata)
    print(f"completed reading the raw json file:- {document_name}")

    df1 = pd.DataFrame()
    print(f"conversion of json to pandas df is started:- {document_name}")
    for x in jsn:
        df = pd.json_normalize(x['Blocks'])
        df1 = pd.concat([df1, df])

    csv_buf = StringIO()
    df1.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    print(f"conversion of json to pandas df is compelted:- {document_name}")

    converted_file_name = file_name.replace(
        "raw-json/", "json-csv/").replace(".json", ".csv")
    print(f"csv file is being saved:- {document_name}")
    resp = s3.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(), Key=converted_file_name)
    print(f'output saved for json to csv conversion {converted_file_name}')
    print(f"csv file is saved:- {document_name}")


    try:
        print(f'format conversion of python response to java response started:- {document_name}')
        java_json_data = convert_format(jsn)
        stored_java_json = s3.put_object(Bucket=bucket_name, Body=json.dumps(java_json_data),
                                            Key=file_name.replace("raw-json/", "digitize-json/"))
        print(f'format conversion of python response to java response completed:- {document_name}')

        move_pdfs_for_digitization(bucket_name, file_name)

    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in input file {file_name}")
    except Exception as e:
        print(f'Unexcpeted error occured during format conversion of python response to java response {str(e)}')


    return {
        "statusCode": resp['ResponseMetadata']['HTTPStatusCode'],
        "body": json.dumps({
            "message": "output saved..",
        }),
    }
keys=[
      
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
            "key": fr"devoted/zai_medical_records_pipeline/textract-response/raw-json/{key.replace('.pdf','.json')}"
        }
        }
    }
    ]
    }
    lambda_handler(event, '')
