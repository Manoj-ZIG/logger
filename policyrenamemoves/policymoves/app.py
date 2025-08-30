import json
import pandas as pd
import boto3
import requests
import json
import os
from io import StringIO
import io
from datetime import date, datetime
from constants import s3_client, get_auth_token
import moves_delete_s3
import os

def lambda_handler(event, context):
  
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    bucket = os.environ['BUCKET_NAME']
    api = os.environ['API']
    path = os.environ['SECRET_PATH']
    auth_tok = get_auth_token(path)
    client = os.environ["CLIENT_NAME"]
    print(f"s3 bucket of moves file: {s3_bucket}, bucket from template: {bucket}, api: {api}, secret path: {path}, bearer token: {auth_tok}, client folder name: {client}")
    moves_file_name = s3_key.split('/')[-1].split('.')[0]
    payorname = s3_key.split('/')[-1].split('.')[0].split('_')[-2]
    print(payorname)
    payorname_lower = payorname.lower()

    try: 
        response = requests.get(f"{api}payor_info?key={payorname_lower}", headers = {"Authorization":f"Bearer {auth_tok}"})
        print("Response status of the API is ", response)
        payorid = str(json.loads(response.text)[0]["id"])
        print(payorid)
    except Exception as e:
        print("Error: ", e)

    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    text_file_body = response['Body']
    txt_file_string = text_file_body.read().decode('utf-8')
    df = pd.read_csv(StringIO(txt_file_string), sep = '|',na_filter=False)

    moves_delete_s3.move_delete_policies(payorid, payorname, df, s3_key, bucket, api, auth_tok, client, moves_file_name)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Moved Successfully",
        }),
    }