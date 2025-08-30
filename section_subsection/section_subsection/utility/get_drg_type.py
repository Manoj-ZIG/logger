import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

def get_drg_type(s3_c, bucket_name, ground_truth_const, file_name):
    ground_truth_const_ = f'{ground_truth_const}{file_name.replace(".csv","")}_ground_truth_constants.csv'
    grd_obj = s3_c.get_object(
        Bucket=bucket_name, Key=ground_truth_const_)
    grd_body = grd_obj['Body']
    grd_body_string = grd_body.read().decode('utf-8')
    grd_df = pd.read_csv(StringIO(grd_body_string))
    flag = 'type' in  grd_df.columns and grd_df.iloc[0]['type']
    return grd_df.iloc[0]['type'] if flag else None