import boto3
import botocore

def key_exists(s3_c, bucket, key):
    try:
        s3_c.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        return False