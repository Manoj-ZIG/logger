import boto3
import pandas as pd
import re
from datetime import date
import requests
import json

DOWNLOAD_BUCKET_NAME = 'zai-reference-data'

s3_client = boto3.client('s3', region_name='us-east-1')
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

def get_auth_token(secretpath):
    auth_token = ''
    secret_name = secretpath
    try:
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_values = response['SecretString']
        credentials = json.loads(secret_values)
        print("Fetching credentials from secret manager successful")
        userId = credentials['userId']
        endpoint = credentials['endpoint']
        apiKey = credentials['apiKey']
        headers = {
        "X-API-Key": apiKey
        }

        payload = {
            "email": userId
            }
        res = requests.post(f"{endpoint}/getAuthKeyToken", json = payload, headers = headers)
        auth_token = res.json()["accessToken"]
        print("Fetching auth token successful")
    except Exception as e:
        print(f"Error during api call: {e}")
    return auth_token

SOURCE_FOLDER_LEVEL_1 = 'zigna'
SOURCE_FOLDER_LEVEL_2 = 'policy_catalog'
SOURCE_FOLDER_LEVEL_4 = 'production_data'
SOURCE_FOLDER_LEVEL_5 = date.today()
SOURCE_FOLDER_LEVEL_6 = 'download'
SOURCE_FOLDER_LEVEL_7 = 'policies'

DEST_FOLDER_LEVEL_1 = 'zigna'
DEST_FOLDER_LEVEL_2 = 'policy_catalog'
DEST_FOLDER_LEVEL_4 = 'production_data'
DEST_FOLDER_LEVEL_5 = '00_final_policies'

FINAL_FILE_FOLDER_LEVEL_2 = 'policy_catalog'
FINAL_FILE_FOLDER_LEVEL_3 = 'approved_policies'


