import json
import cms_mcd
import devoted
import uhc
import transmittal
import centene_download
import boto3
import pandas as pd
from constants import s3_client
import os

def lambda_handler(event, context):
    
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    batch = s3_key.split('/')[-1].split('_')[0]
    cms_mcd_category = s3_key.split('/')[-1].split('_')[1]
    cms_batch = s3_key.split('/')[-1].split('_')[1]

    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_csv(response['Body'])

    if df['extrctn_client_name'][0] == 'devoted':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"] 
        print(f"Download of Devoted batch {batch} is started")  
        devoted.download_files(df,batch, BUCKET_1_NAME, API_1_NAME)
           
    if df['extrctn_client_name'][0] == 'cms': 
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]
        print(f"Download of CMS batch {batch} is started")   
        cms_mcd.download_files(df,batch, cms_mcd_category, BUCKET_1_NAME, API_1_NAME)
    
    if df['extrctn_client_name'][0] == 'uhc':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]
        print(f"Download of UHC batch {batch} is started")
        uhc.download_files(df,batch, BUCKET_1_NAME, API_1_NAME)

    if df['extrctn_client_name'][0] == 'Transmittals_and_MLN_Articles':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]
        print(f"Download of transmittals and mln articles batch {batch} is started")
        transmittal.download_files(df,batch, BUCKET_1_NAME, API_1_NAME)

    if df['extrctn_client_name'][0] == 'Publications':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]
        print(f"Download of publications batch {batch} is started")
        transmittal.download_files(df,batch, BUCKET_1_NAME, API_1_NAME)
    
    if df['extrctn_client_name'][0] == 'centene':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]
        print(f"Download of centene batch {batch} is started")
        centene_download.download_files(df,batch, BUCKET_1_NAME, API_1_NAME)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }