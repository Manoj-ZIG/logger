from io import StringIO
import pandas as pd


def read_csv_from_s3(s3_client, bucket_name, csv_file_key, usecols=[], na_filter=False):
    csv_data_obj = s3_client.get_object(
        Bucket=bucket_name, Key=csv_file_key)
    csv_data_body = csv_data_obj['Body'].read().decode('utf-8')
    if usecols:
        csv_data = pd.read_csv(
            StringIO(csv_data_body),  na_filter=na_filter, usecols=usecols)
    else:
        csv_data = pd.read_csv(
            StringIO(csv_data_body), na_filter=na_filter)
    return csv_data

def read_csv_file(s3c, bucket_name_const, key, encoding, dtype = None):
    """
    ### The function reads the claim template map constant file
    args:
        s3_c: s3_client
        bucket_name: bucket_name
        key: key
    return:
        df: DataFrame
    """
    path_for_csv = key
    csv_obj = s3c.get_object(
        Bucket=bucket_name_const, Key=rf"{path_for_csv}")
    csv_body = csv_obj['Body']
    csv_string = csv_body.read().decode('utf-8')
    if dtype:
        csv_data = pd.read_csv(StringIO(csv_string),encoding=encoding, dtype=str)
    else:
        csv_data = pd.read_csv(StringIO(csv_string), encoding=encoding)
    return csv_data

def put_object_to_s3(s3_client, bucket_name, save_path, file_name, df):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    s3_client.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
    ), Key=f'{save_path}/{file_name.replace(".csv",".csv")}')
    csv_buf.seek(0)


def copy_object_to_s3(s3c, bucket, copy_source, copy_key):
    s3c.copy_object(CopySource=copy_source, Bucket=bucket,
                    Key=copy_key)
    print(f'copied {copy_source} to the key:  {copy_key}')
    return 'copied'
