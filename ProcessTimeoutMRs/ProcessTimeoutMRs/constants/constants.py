import json
# VITALS_LAMBDA_LOG_GROUP_NAME = "/aws/lambda/vitalLabExtraction-prod"
# POSTPROCESSING_LAMBDA_LOG_GROUP_NAME = "/aws/lambda/rawDataPostprocess-prod"

# vitals_lambda_script_path = "/home/ubuntu/vital_lab_extraction/vital_lab_extraction/app.py"
# postprocessing_lambda_script_path = "/home/ubuntu/raw_data_postprocess/raw_data_postprocess/app.py"
# meta_dict = {"/aws/lambda/vitalLabExtraction-prod":"/home/ubuntu/vital_lab_extraction/vital_lab_extraction/app.py",
# "/aws/lambda/rawDataPostprocess-prod":"/home/ubuntu/raw_data_postprocess/raw_data_postprocess/app.py"}
queue = f"zai_medical_records_pipeline/medical-records-extract/workflow/queue"
processing = f"zai_medical_records_pipeline/medical-records-extract/workflow/processing"
processed = f"zai_medical_records_pipeline/medical-records-extract/workflow/processed"
error = f"zai_medical_records_pipeline/medical-records-extract/workflow/error"
lookBackTime = 360

def delete_object(s3c, bucket, key):
    '''
    ### Deletes the file present in the s3 bucket

    args:
        s3c: s3 client
        bucket_name: string (Name of the S3 bucket)
        key: string (path of the file)
    return:
        None
    '''
    s3c.delete_object(Bucket=bucket, Key=key)
    print(f'Deleted the document : {key}')

def put_json_file(s3c, path_to_save_result, json_file_name, json_data, bucket_name):
    '''
    ### Puts the json file in the s3 bucket

    args:
        s3c: s3 client
        path_to_save_result: string (s3 path to store the result)
        json_file_name: string (file name)
        bucket_name: string (Name of the S3 bucket)        
    return:
        return_dict: json (meta data)
        source_key: json (destination bucket and key)
    '''
    s3c.put_object(Bucket=bucket_name, Body=json.dumps(json_data),
                            Key=rf'{path_to_save_result}/{json_file_name}')

    source_key = {'Bucket': bucket_name,
                    'Key': rf'{path_to_save_result}/{json_file_name}'}
    return source_key

def copy_object(s3c, bucket, key, dest_bucket, dest_key):
    '''
    ### Copies the s3 file to another location
    
    args:
        s3c: s3 client
        bucket: string (Name of the S3 bucket)
        key: string (path of the file)
        dest_key: string (destination path of the file)
        output_bucket: string (Name of the output S3 bucket)
    return:
        None
    '''
    copy_source = {
        'Bucket': bucket,
        'Key': key,
    }
    s3c.copy(copy_source,dest_bucket, dest_key)
    print(f'Copied the processed document {key} to {dest_key}')

def list_all_objects(s3r, bucket, s3_path):
    '''
    ### Lists the file present in the s3 bucket

    args:
        s3r: s3 resource
        bucket_name: string (Name of the S3 bucket)
        s3_path: string (path of the file)
    return:
        ls: List of objects present in the given s3_path
    '''
    keys, files = [], []
    object = s3r.Bucket(bucket).objects.filter(Prefix=s3_path)
    for obj in object:
        if obj.key[-1] != "/":
            keys.append(obj.key)
            files.append(obj.key.split("/")[-1])
    return keys, files

def read_json(s3c, bucket_name, key):
    json_object = s3c.get_object(Bucket=bucket_name,
                                      Key=key)
    json_body = json_object["Body"].read().decode('utf-8')
    json_data = json.loads(json_body)
    return json_data

def check_instance_status(ec2c, instance_id):
    print(f'checking ec2 instance status')
    response=ec2c.describe_instance_status( InstanceIds=[instance_id,],)
    print(f'RESPONSE: {response}')
    if 'InstanceStatuses' in response and len(response['InstanceStatuses'])>0:
        status=response['InstanceStatuses'][0]['InstanceState']['Name']
        return status 
    else:
        return None