import json
import boto3
import pandas as pd
from io import BytesIO, StringIO
# workgroup = 'mr_processing_queries'
workgroup = 'primary'
query_output_location = 's3://zai-revmax-qa/athena_mr_processing_logs/'

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

def copy_object(s3c, bucket, key, output_bucket, dest_key):
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
    s3c.copy(copy_source,output_bucket, dest_key)
    print(f'Copied the processed document {key} to {dest_key}')

def read_json_from_s3(s3c, bucket_name, json_file_path):
    '''
    ### Reads the json file
    
    args:
        s3c: s3 client
        bucket: string (Name of the S3 bucket)
        json_file_path: string (path of the file)
    return:
        json_data: json output
    '''
    response = s3c.get_object(Bucket=bucket_name,Key=json_file_path)
    content = response['Body'].read().decode('utf-8')
    json_data = json.loads(content)
    return json_data

def list_all_objects(s3r, bucket, s3_path, document_type):
    '''
    ### Lists the file present in the s3 bucket

    args:
        s3r: s3 resource
        bucket_name: string (Name of the S3 bucket)
        s3_path: string (path of the file)
        document_type: strin (.txt, .parquet, .csv)
    return:
        ls: List of objects present in the given s3_path
    '''
    keys = []
    object = s3r.Bucket(bucket).objects.filter(Prefix=s3_path)
    for obj in object:
        if obj.key.endswith(document_type) and obj.key[-1] != "/":
            keys.append(obj.key)
    return keys

def read_files(s3c, bucket_name, keys):
    '''
    ### Reads the file present in the s3 bucket

    args:
        s3c: s3 resource
        bucket_name: string (Name of the S3 bucket)
        keys: list (path of the file)
    return:
        df: dataframe
    '''
    combined_df = pd.DataFrame({})
    for key in keys:
        if key.endswith('.txt'):
            response = s3c.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(content), delimiter='|')
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        if key.endswith('.parquet'):
            response = s3c.get_object(Bucket=bucket_name, Key=key)
            parquet_content = response['Body'].read()
            df = pd.read_parquet(BytesIO(parquet_content))
            combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df

def upload_df_to_s3_as_pipe_delimited(s3c, df, bucket_name, key):
    '''
    Uploads a DataFrame to S3 as a pipe-delimited CSV file.

    args:
        s3c: s3 client
        df: DataFrame to upload
        bucket_name: string (Name of the S3 bucket)
        key: string (Path and file name in the S3 bucket)
    '''
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)
    s3c.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
    print(f"DataFrame uploaded as pipe-delimited CSV file to {bucket_name}/{key}")

def upload_df_to_s3_as_pipe_delimited_txt(s3c, df, bucket_name, key):
    '''
    Uploads a DataFrame to S3 as a pipe-delimited TXT file.

    args:
        s3c: s3 client
        df: DataFrame to upload
        bucket_name: string (Name of the S3 bucket)
        key: string (Path and file name in the S3 bucket)
    '''
    txt_buffer = StringIO()
    df.to_csv(txt_buffer, sep='|', index=False, na_rep='')
    s3c.put_object(Bucket=bucket_name, Key=key, Body=txt_buffer.getvalue())
    print(f"DataFrame uploaded as pipe-delimited TXT file to {bucket_name}/{key}")