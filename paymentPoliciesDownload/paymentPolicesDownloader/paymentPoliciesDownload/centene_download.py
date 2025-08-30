from datetime import date,datetime
import pandas as pd
import requests
import boto3
import io
from constants import s3_client
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_8,DEST_FOLDER_LEVEL_9,DEST_FOLDER_LEVEL_10, EXTRACT_FOLDER
import re
import random
import time

DEST_FOLDER_LEVEL_3 = 'centene' 
client_name = 'centene'

def update_status(df, pdf_link, s3_path, status_code,modified_file_name, bucket): 
    print('length of df from update status is: ', len(df))
    s3_link = f"https://{bucket}.s3.amazonaws.com/{s3_path}"
    s3_path = f's3://{bucket}/{s3_path}'
    print(pdf_link)
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_s3_uri'] = s3_path
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_s3_url'] = s3_link
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_status_code'] = status_code
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_file_name'] = modified_file_name
    print(df.loc[df['extrctn_pdf_links'] == pdf_link].shape)

def download_files(df,batch, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        random_number = random.randint(30, 60)
        print(random_number)
        print('sleep starts')
        time.sleep(random_number)
        print('sleep ends')
        downloadable_links = list(set(df['extrctn_pdf_links']))
        print(len(downloadable_links))
        s3_key_extract_raw = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{EXTRACT_FOLDER}/{DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
        response = s3_client.get_object(Bucket=bucket, Key=s3_key_extract_raw)
        extract_df = pd.read_csv(response['Body'])
        print("length of extract raw is: ", len(extract_df))
        for link in downloadable_links:
            try:
                response = requests.get(link)
                if response.status_code == 200:
                    fname = "_".join(link.split('/')[2:]).replace('.pdf','').replace('%20','_').replace('www.','').replace('.com','')
                    pattern = re.compile('[^a-zA-Z0-9]+')
                    file_name = re.sub(pattern, '_', fname) + '.pdf'
                    s3_path = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_8}/{file_name}'
                    s3_client.put_object(Bucket=bucket, Key=s3_path, Body=response.content, ContentType='application/pdf')
                    update_status(extract_df, link,s3_path, response.status_code,file_name, bucket)
                    print(f'Success: policy downloaded to bucket {bucket}')
                else:
                    update_status(extract_df, link, 'NA', response.status_code,"NA", bucket)
                    print(f'Response not 200 - {response.status_code}')
            except Exception as e:
                    update_status(extract_df, link, 'NA', response.status_code,"NA", bucket)
                    print(f'Response or logic Failed - {e}')
        print("Extract mapped df shape before dropping NA is: ", extract_df.shape)            
        extract_df = extract_df.dropna(subset=['dwnld_status_code'])
        print("Extract mapped df shape after dropping NA is: ", extract_df.shape)
        
        csv_buffer = io.BytesIO()
        extract_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_9}/{batch}_{DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_download_status.csv'
        s3_client.upload_fileobj(csv_buffer, bucket, out_filename)  
        print(f"Files uploaded to S3 bucket {bucket} successfully")

        batch_size = 100
        for i in range(0, len(extract_df['extrctn_pdf_links'].unique()), batch_size):
            pdf_list = (extract_df['extrctn_pdf_links'].unique().tolist())[i:i+batch_size]
            subset_df = extract_df[extract_df["extrctn_pdf_links"].isin(pdf_list)]
            batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_10}/{batch}_{i}_{DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_abstract_trigger.csv'
            batch_csv_buffer = io.BytesIO()
            subset_df.to_csv(batch_csv_buffer, index=False)
            batch_csv_buffer.seek(0)
            s3_client.upload_fileobj(batch_csv_buffer, bucket, batch_key)
            print(f"Abstract trigger batch {i} file uploaded to bucket {bucket}")
        return "Files uploaded to S3 bucket"