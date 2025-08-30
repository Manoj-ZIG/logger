import json
import boto3
import requests

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