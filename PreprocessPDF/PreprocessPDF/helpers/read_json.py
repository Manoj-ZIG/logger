import json

def read_json(s3_c, bucket_name, textract_json_path):
    """
    ### The function reads the json file
    args:
        s3_c: s3_client
        bucket_name: bucket_name
        textract_json_path: key
    return:
        json_dict: json
    """
    json_object = s3_c.get_object(Bucket=bucket_name,
                                        Key=f'{textract_json_path}')
    json_body = json_object["Body"].read().decode('utf-8')
    json_dict = json.loads(json_body)
    return json_dict