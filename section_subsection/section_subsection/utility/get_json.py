import json

def read_json(s3_c, bucket_name, key):
    json_object = s3_c.get_object(Bucket=bucket_name,
                                      Key=key)
    json_body = json_object["Body"].read().decode('utf-8')
    api_constant = json.loads(json_body)
    return api_constant