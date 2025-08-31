import json
import urllib.parse
import boto3
from constants.aws_config import aws_access_key_id,aws_secret_access_key
from helpers.custom_logger import enable_custom_logging

enable_custom_logging()
def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    document_name = key.split('/')[-1]
    print(bucket, key)
    entity = 'Other'
    if 'digitized-pdf' in key :
        try:
            textract = boto3.client('textract', region_name='us-east-1',
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
            
            print(f"text extraction starting:- {document_name}")
            response = textract.start_document_text_detection(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': str(key)
                    }
                },
                JobTag='pdf_text',
                NotificationChannel={
                    'RoleArn':  'arn:aws:iam::833984991867:role/lambda-textract-s3-assume-role',
                    'SNSTopicArn': 'arn:aws:sns:us-east-1:833984991867:AmazonTextract-lambda'

                    #'SNSTopicArn': 'arn:aws:sns:us-east-1:833984991867:text_extracted_notification'
                })
            print(f"SNS notfication sent:- {document_name}")
            
            print('Triggered PDF Processing for ' + key)
            print(response['JobId'])
            entity = 'pdf_text'
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
    elif 'textract_table_merged' in key:
        try:
            textract = boto3.client('textract', region_name='us-east-1',
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
            print(f"table extraction starting:- {document_name}")


            response = textract.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': key
                    }
                },
                FeatureTypes=['TABLES'],
                JobTag='pdf_table',
                NotificationChannel={
                    'RoleArn':  'arn:aws:iam::833984991867:role/lambda-textract-s3-assume-role',
                    # 'RoleArn':  'arn:aws:iam::833984991867:role/Delete_Role',
                    'SNSTopicArn': 'arn:aws:sns:us-east-1:833984991867:AmazonTextract-lambda'
                }
            )
            
            print(f"SNS notfication sent for tables:- {document_name}")

            print('Triggered Merged PDF table extraction processing for:' + key)
            print(response['JobId'])
            entity = 'pdf_table'
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
    else:
        print('False alarm due to object creation in processed folder')
    
    print(entity)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"generated jobId for {entity}",

        }),
    }



keys=[

'devoted/zai_medical_records_pipeline/concat-files/digitized-pdf/ed4b9177-3f4f-4aca-a203-3ce25b499bd1_AJX27GW4KC_IP1_20250703_152838.pdf'
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
            "key": key
        }
        }
    }
    ]
    }
    lambda_handler(event, '')
