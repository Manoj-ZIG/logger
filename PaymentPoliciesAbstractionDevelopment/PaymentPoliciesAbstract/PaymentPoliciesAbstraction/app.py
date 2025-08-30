import json
import pandas as pd
import boto3
import devoted_abstract
import cms_mcd_abstract

# import uhc_abstract
# import transmittals_articles_abstract
# import centene
import os
from datetime import datetime
from constants import s3_client

def lambda_handler(event, context):
  
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    batch_0 = s3_key.split('/')[-1].split('_')[0]
    batch_1 = s3_key.split('/')[-1].split('_')[1]

    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_csv(response['Body'])

    if df['extrctn_client_name'][0] == 'cms':
        if batch_0 == '0' and batch_1 == '0':
            BUCKET_1_NAME = os.environ["BUCKET_NAME"]
            API_1_NAME = os.environ["API"]   
            SECRET_PATH_1 = os.environ["SECRET_PATH"]
            CLIENT = os.environ["CLIENT_NAME"]
            print(f'Abstraction of CMS is started')
            cms_mcd_abstract.save_file_to_s3(batch_0,batch_1, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1, CLIENT, out_filename=None)

    if df['extrctn_client_name'][0] == 'devoted':
        BUCKET_1_NAME = os.environ["BUCKET_NAME"]
        API_1_NAME = os.environ["API"]  
        SECRET_PATH_1 = os.environ["SECRET_PATH"]
        CLIENT = os.environ["CLIENT_NAME"]
        print(f'Abstraction of devoted batch {batch_0}_{batch_1} is started')
        attributes_list, bucket_name, api_name, env_secret =  devoted_abstract.extract_attributes(df, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1)
        print(f'Attribute list generated for devoted batch: {batch_0}_{batch_1}')
        devoted_abstract.save_file_to_s3(df,attributes_list,batch_0,batch_1, bucket_name, api_name, env_secret, CLIENT, out_filename=None)

    # if df['extrctn_client_name'][0] == 'uhc':
    #     BUCKET_1_NAME = os.environ["BUCKET_NAME"]
    #     API_1_NAME = os.environ["API"]   
    #     SECRET_PATH_1 = os.environ["SECRET_PATH"]
    #     CLIENT = os.environ["CLIENT_NAME"]
    #     print(f'Abstraction of batch {batch_0}_{batch_1} is started')
    #     attributes_list, bucket_name, api_name, env_secret =  uhc_abstract.extract_attributes(df, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1)
    #     print(f'Attribute list generated for batch: {batch_0}_{batch_1}')
    #     uhc_abstract.save_file_to_s3(df,attributes_list,batch_0,batch_1, bucket_name, api_name, env_secret, CLIENT, out_filename=None)

    # if df['extrctn_client_name'][0] == 'Transmittals_and_MLN_Articles':
    #     BUCKET_1_NAME = os.environ["BUCKET_NAME"]
    #     API_1_NAME = os.environ["API"]   
    #     SECRET_PATH_1 = os.environ["SECRET_PATH"]
    #     CLIENT = os.environ["CLIENT_NAME"]
    #     print(f'Abstraction of batch {batch_0}_{batch_1} of Transmittals_and_MLN_Articles is started')
    #     attributes_list, bucket_name, api_name =  transmittals_articles_abstract.extract_attributes(df, BUCKET_1_NAME, API_1_NAME)
    #     transmittals_articles_abstract.save_file_to_s3(df,attributes_list,batch_0,batch_1, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1, CLIENT, out_filename=None)

    # if df['extrctn_client_name'][0] == 'Publications':
    #     BUCKET_1_NAME = os.environ["BUCKET_NAME"]
    #     API_1_NAME = os.environ["API"]   
    #     SECRET_PATH_1 = os.environ["SECRET_PATH"]
    #     CLIENT = os.environ["CLIENT_NAME"]
    #     print(f'Abstraction of batch {batch_0}_{batch_1} of Publications is started')
    #     attributes_list, bucket_name, api_name =  transmittals_articles_abstract.extract_attributes(df, BUCKET_1_NAME, API_1_NAME)
    #     transmittals_articles_abstract.save_file_to_s3(df,attributes_list,batch_0,batch_1, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1, CLIENT, out_filename=None)

    # if df['extrctn_client_name'][0] == 'centene':
    #     BUCKET_1_NAME = os.environ["BUCKET_NAME"]
    #     API_1_NAME = os.environ["API"]   
    #     SECRET_PATH_1 = os.environ["SECRET_PATH"]
    #     CLIENT = os.environ["CLIENT_NAME"]
    #     print(f'Abstraction of batch {batch_0}_{batch_1} is started')
    #     attributes_list, bucket_name, api_name, env_secret =  centene.extract_attributes(df, BUCKET_1_NAME, API_1_NAME, SECRET_PATH_1)
    #     print(f'Attribute list generated for batch: {batch_0}_{batch_1}')
    #     centene.save_file_to_s3(df,attributes_list,batch_0,batch_1, bucket_name, api_name, env_secret, CLIENT, out_filename=None)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }
