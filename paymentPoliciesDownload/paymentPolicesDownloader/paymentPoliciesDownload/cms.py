from datetime import datetime,date
import pandas as pd
import requests
import boto3
import io
from constants import BUCKET_NAME,s3_client

def download_files(downloadable_links,batch):

    bucket_name = BUCKET_NAME
    folder_name = 'PaymentPolicies'
    download_date = date.today()
    client_name = 'CMS'

    status = {'pdf_link':[],'s3_path':[],'status':[]}
    s3 = boto3.client('s3')
    downloadable_links = downloadable_links[['pdf_links','line_of_business','file_type']].drop_duplicates()

    for i,link in downloadable_links.iterrows():
        lob = link['line_of_business']
        try:
            if 'https://www.cms.gov/files/' in str(link['pdf_links']) or 'https://www.cms.gov/medicare/' in str(link['pdf_links']):
                response = requests.get(link['pdf_links'])
                if response.status_code == 200:
                    if link['file_type'] == 'pdf':
                        file_name = link['pdf_links'].split('/')[-1]
                        s3_path = f"{folder_name}/{client_name}/{download_date}/policies/{lob}/{file_name}"

                        # Upload the file to S3 bucket
                        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
                        status['pdf_link'].append(link['pdf_links'])
                        s3_link = f"https://{bucket_name}.s3.amazonaws.com/{s3_path}"
                        status['s3_path'].append(s3_link)
                        status['status'].append('Success')
                        print('Success')
                    elif link['file_type'] == 'zip':
                        file_name = "_".join(link['pdf_links'].split('/')).split('zip_')[1]
                        s3_path = f"{folder_name}/{client_name}/{download_date}/policies/{lob}/{file_name}"

                        # Upload the file to S3 bucket
                        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
                        status['pdf_link'].append(link['pdf_links'])
                        s3_link = f"https://{bucket_name}.s3.amazonaws.com/{s3_path}"
                        status['s3_path'].append(s3_link)
                        status['status'].append('Success')
                        print('Success')
                    elif link['file_type'] == 'xlsx':
                        file_name = link['pdf_links'].split('/')[-1]
                        s3_path = f"{folder_name}/{client_name}/{download_date}/policies/{lob}/{file_name}"

                        # Upload the file to S3 bucket
                        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
                        status['pdf_link'].append(link['pdf_links'])
                        s3_link = f"https://{bucket_name}.s3.amazonaws.com/{s3_path}"
                        status['s3_path'].append(s3_link)
                        status['status'].append('Success')
                        print('Success')
                    elif link['file_type'] == 'csv':
                        file_name = link['pdf_links'].split('/')[-1]
                        s3_path = f"{folder_name}/{client_name}/{download_date}/policies/{lob}/{file_name}"

                        # Upload the file to S3 bucket
                        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
                        status['pdf_link'].append(link['pdf_links'])
                        s3_link = f"https://{bucket_name}.s3.amazonaws.com/{s3_path}"
                        status['s3_path'].append(s3_link)
                        status['status'].append('Success')
                        print('Success')
                else:
                    print(f'Failed - {response.status_code}')

        except Exception as e:
            print(e)
            status['pdf_link'].append(link['pdf_links'])
            status['s3_path'].append('')
            status['status'].append(f'Failed - {e}')
            print('Error')
    status_df = pd.DataFrame(status)
    # Create a csv file
    csv_buffer = io.BytesIO()
    status_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    out_filename = f'PaymentPoliciesStatus/{download_date}/{client_name}/{batch}_payment_policies_status.csv'
    s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename)    
    print("Files uploaded to S3 bucket successfully")
    return "Files uploaded to S3 bucket"