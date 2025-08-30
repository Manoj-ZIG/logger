from datetime import date,datetime
import pandas as pd
import requests
import boto3
import io
from constants import s3_client
import re
from bs4 import BeautifulSoup
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_8,DEST_FOLDER_LEVEL_9,DEST_FOLDER_LEVEL_10

PAYOR_NAME_DEST_FOLDER_LEVEL_3 = 'cms'
client_name = 'cms'

def update_status(df,pdf_link, s3_path, status_code, modified_file_name, bucket):
    
    s3_link = f"https://{bucket}.s3.amazonaws.com/{s3_path}"
    s3_path = f's3://{bucket}/{s3_path}'
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_s3_uri'] = s3_path
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_s3_url'] = s3_link
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_status_code'] = status_code
    df.loc[df['extrctn_pdf_links'] == pdf_link, 'dwnld_file_name'] = modified_file_name

def download_files(df,batch, category, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        downloadable_links = list(set(df['extrctn_pdf_links']))
        for link in downloadable_links:
            try:
                response = requests.get(link)
                if response.status_code == 200:
                    text = response.text
                    soup = BeautifulSoup(text, 'html.parser')
                    for tag in soup.find_all(True):
                        if tag.has_attr('id') and tag['id'] in ['lblLcdId', 'lblArticleId', 'lblLcdTitle', 'lblArticleTitle', 'lblManualSectionTitle', 
                                                        'lblOriginalEffectiveDate', 'lblEffectiveDate', 'lblRevisionEffectiveDate', 'lblRetirementDate', 
                                                        'lblEndingEffectiveDate', 'pnlCodingInformation', 'pnlContractorInformation']:
                            continue
                        tag.attrs = {}
                    text = soup.prettify()
                    text = re.sub(r'<head>.*?</head>', '', text, flags=re.DOTALL)
                    text = re.sub(r'<body.*?(<h1)', r'\1', text, flags=re.DOTALL)
                    text = re.sub(r'Email this document to yourself or someone else.*?</body>', '', text, flags=re.DOTALL)

                    file = str(df[df['extrctn_pdf_links']==link]['extrctn_file_name'].values[0]) 
                    fname = file.replace('.html','').replace('%20','_') 
                    pattern = re.compile('[^a-zA-Z0-9]+')
                    file_name = re.sub(pattern, '_', fname) + '.html' 
                    id_ver = str(link.split('&')[0].split("=")[-1]) +'_'+ str(link.split('&')[1].split("=")[-1])
                    modified_file_name = f'{id_ver}_{file_name}'

                    s3_path = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_8}/{modified_file_name}'
                    s3_client.put_object(Bucket=bucket, Key=s3_path, Body=text.encode('utf-8'), ContentType='text/html')
                    update_status(df, link,s3_path, response.status_code, modified_file_name, bucket)
                    print(f'Success: policy downloaded to bucket {bucket}')
                else:
                    update_status(df, link, 'NA', response.status_code, 'NA', bucket)
                    print(f'Response not 200 - {response.status_code}')
            except Exception as e:
                    update_status(df, link, 'NA', response.status_code, 'NA', bucket)
                    print(f'Response or logic Failed - {e}')

        # Create a csv file
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_9}/{batch}_{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_download_status.csv'
        s3_client.upload_fileobj(csv_buffer, bucket, out_filename)  
        print(f"Files uploaded to S3 bucket {bucket} successfully")

        batch_size = 100
        batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
        # Store each batch in S3
        for i, btch in enumerate(batches):
            batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_10}/{batch}_{i}_{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_abstract_trigger.csv'
            batch_csv_buffer = io.BytesIO()
            btch.to_csv(batch_csv_buffer, index=False)
            batch_csv_buffer.seek(0)
            s3_client.upload_fileobj(batch_csv_buffer, bucket, batch_key)
            print(f"Abstract trigger batch {i} file uploaded to bucket {bucket}")
        return "Files uploaded to S3 bucket"