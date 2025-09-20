import builtins
import csv
import os
from datetime import datetime
import re
import boto3
from io import BytesIO
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
try:
    from constants.aws_config import aws_access_key_id,aws_secret_access_key
except:
    from ..constants.aws_config import aws_access_key_id,aws_secret_access_key




category={
    'section extraction':'Section extraction',
    'subsections extraction':'Subsection extraction',
    'chart order extraction':'Chart order extraction'


}



def custom_print(*args, **kwargs):
    timestamp = datetime.now().isoformat(timespec='milliseconds')
    message = " ".join(str(arg) for arg in args)
    builtins._original_print(message, **kwargs)

    if ":-" in message:
        pattern = r'(AJX[\w\-]{7}|H00[\w\-]{9}|V00[\w\-]{9}|200[\w\-]{4}|IB[\w\-]{2,})'
        ARL = re.findall(pattern, message, flags=re.IGNORECASE)
        ARL = ARL[0] if ARL else ''
        ARL = ARL.upper()
        if ARL.startswith(("AJX","IB")):
            client = "devoted"
        elif ARL.startswith(("H00", "V00", "22")):
            client = "helix"
        else:
            client = ""
        


        file_name_part = message.split(':-')[-1].strip()
        Message = message.split(':-')[0].strip()

        category_ = ""
        for key in category:
            if key.lower() in message.lower():
                category_ = category[key]
                break
        if category_:
            # Prepare pipe-delimited content
            headers = "timestamp|lambda|arl|file|category|message"
            values = f"{timestamp}|sectionSubsection|{ARL}|{file_name_part}|{category_}|{Message}"
            content = f"{headers}\n{values}"

            # Create a TXT file in memory
            buffer = BytesIO()
            buffer.write(content.encode('utf-8'))
            buffer.seek(0)

            # Upload to S3
            date_folder = datetime.now().strftime("%Y%m%d")
            timestamp_for_file = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{client}/{S3_FOLDER}/{date_folder}/{file_name_part.replace('.pdf','')}_{category_}_{timestamp_for_file}.txt"
            try:
                s3c.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer)
            except Exception as e:
                builtins._original_print(f"Failed to upload Parquet to S3: {e}")


            
            

def enable_custom_logging():
    builtins._original_print = builtins.print
    builtins.print = custom_print

def disable_custom_logging():
    builtins.print = builtins._original_print

S3_BUCKET = os.environ['PARAMETERS_BUCKET_NAME']
S3_FOLDER = f"execution_trace_logs"

try:
    s3c = boto3.client('s3', region_name='us-east-1')
    s3c.list_buckets()
    print("S3 client initialized successfully using IAM role in app.py.")
except Exception as e:
    print(f"Failed to initialize S3 client with IAM role: {str(e)} in app.py.")
    if aws_access_key_id and aws_secret_access_key:
        s3c = boto3.client('s3', 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

