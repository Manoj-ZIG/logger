import re
import io
from io import BytesIO
import pandas as pd
import time
from constants import COLUMN_PREFIX, TIMEDELTA,add_column_prefix, s3_client,DOWNLOAD_BUCKET_NAME, CMS_MCD_ZIP_URL
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7
from datetime import date, timedelta, datetime
import compare_policies
from email_body import email_body, send_email_via_ses
import requests

error_links = []
PAYOR_NAME_DEST_FOLDER_LEVEL_3 = 'cms' 
client_name = 'cms'
download_date =  date.today()

def download_file(CMS_MCD_ZIP_URL):

    response = requests.get(CMS_MCD_ZIP_URL)
    if response.status_code == 200:
        try:
            zip_content = BytesIO(response.content)
            file_name = "reference_files/" + "CMS_MCD_POLICIES/" + str(DEST_FOLDER_LEVEL_5) + "/" + CMS_MCD_ZIP_URL.split('/')[-1]
            s3_client.upload_fileobj(zip_content, DOWNLOAD_BUCKET_NAME, file_name)
            print(f"Downloaded {file_name} in {DOWNLOAD_BUCKET_NAME}")
            return 200
        except Exception as e:
            print(e)
            return 404
    else:
        print(f"Failed to download {CMS_MCD_ZIP_URL}")
        return 404
    
def generate_links(category,links):

    url_data = []
    response = s3_client.get_object(Bucket=links[0], Key=links[1])
    csv_bytes = response['Body'].read()
    csv_data = io.BytesIO(csv_bytes)
    df = pd.read_csv(csv_data,dtype = str, encoding_errors='ignore')

    for index, row in df.iterrows():
        try:
            temp = {}
            link = r'https://www.cms.gov/medicare-coverage-database/view/{}.aspx?{}={}&{}={}'.format(links[2], links[3], int(row[links[5]]), links[4], int(row[links[6]]))
            file_name = str(row[links[7]]).replace('%20', '-')
            file_name = re.sub(r'[^a-zA-Z0-9]+', '_', file_name)
            temp['category'] = category
            temp['pdf_links'] = link
            temp['id'] = int(row[links[5]])
            temp['version'] = int(row[links[6]])
            temp['file_name'] = file_name
            temp['client_name'] = client_name
            temp['site_name'] = '/'.join(link.split('/')[:3])
            temp['url'] = CMS_MCD_ZIP_URL
            temp['sub_url'] = links[1]
            temp['download_date'] = date.today()
            temp['effective_date'] = str(row[links[8]])
            temp['state'] = links[9]
            temp['line_of_business'] = 'Medicare'
            temp['policy_type'] = links[10]
            temp['status'] = "no change"
            url_data.append(temp) 
        except:
            error_links.append((link,200))
    df1 = pd.DataFrame(url_data)
    return df1

def save_policy_files_to_s3(CMS_MCD_ZIP_URL,CMS_MCD_BASE_URLS, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        rcode = download_file(CMS_MCD_ZIP_URL)
        time.sleep(45)
        if rcode == 200:
            final_df = pd.DataFrame()
            for category,links in CMS_MCD_BASE_URLS.items():
                fdf = generate_links(category,links)
                final_df = pd.concat([final_df,fdf],ignore_index=True)

            final_df['status'] = 'no change'
            download_date =  date.today()
            
            final_df.columns = add_column_prefix(final_df, COLUMN_PREFIX)
            final_df = final_df[['extrctn_client_name','extrctn_site_name','extrctn_url', 'extrctn_sub_url', 'extrctn_pdf_links','extrctn_file_name','extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_policy_type','extrctn_download_date','extrctn_status']]

            # Create a csv file
            csv_buffer = io.BytesIO()
            final_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
            s3_client.upload_fileobj(csv_buffer, bucket, out_filename) 
            print(f'policy extract file uploaded to s3 bucket {bucket}')

            error_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction_base_url_logs.csv'
            error_buffer = io.BytesIO()
            error_df = pd.DataFrame(error_links,columns = ['site','error_code'])
            error_df.to_csv(error_buffer, index=False)
            error_buffer.seek(0)
            s3_client.upload_fileobj(error_buffer, bucket, error_filename)
            print(f'error files uploaded to s3 bucket {bucket}')

            download_date =  date.today()
            LAST_DOWNLOAD_DATE =  str(date.today() - timedelta(days=int(TIMEDELTA)))

            k1 = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{LAST_DOWNLOAD_DATE}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{LAST_DOWNLOAD_DATE}_policy_extraction.csv'
            try:
                response1 = s3_client.get_object(Bucket=bucket, Key=k1)
            except Exception as e:
                print(e)
                response1 = {'ResponseMetadata': {'HTTPStatusCode': 404}}
                
            if response1['ResponseMetadata']['HTTPStatusCode'] == 200:
                compare_policies.check_updated_policy(response1, PAYOR_NAME_DEST_FOLDER_LEVEL_3,PAYOR_NAME_DEST_FOLDER_LEVEL_3,final_df,download_date,bucket,s3_client, out_filename)
            else:                                
                csv_buffer = io.BytesIO()
                final_df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
                s3_client.upload_fileobj(csv_buffer, bucket, out_filename)
                print(f'policy extract file uploaded to s3 bucket {bucket} after skipping compare policy')

                html_body = email_body(PAYOR_NAME_DEST_FOLDER_LEVEL_3, bucket, download_date, final_df, error_df, out_filename)
                html_body = html_body.replace('start',"<html><body>").replace('end',"</body></html>")
                send_email_via_ses(f'{PAYOR_NAME_DEST_FOLDER_LEVEL_3.upper()} - Policy Update - {download_date}',
                                    html_body,
                                    sender_email='rightpx@zignaai.com',recipient_email=['suriya@zignaai.com','netra@zignaai.com', 'gopireddy@zignaai.com', 'connie@zignaai.com', 'sarvesh@zignaai.com'])

                batch_size = 50
                batches = [final_df[i:i+batch_size] for i in range(0, len(final_df), batch_size)]
                # Store each batch in S3
                destination_bucket = bucket
                for i, batch in enumerate(batches):
                    batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_7}/{i}_{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_batch_policies_download.csv'
                    # Create a csv file
                    batch_csv_buffer = io.BytesIO()
                    batch.to_csv(batch_csv_buffer, index=False)
                    batch_csv_buffer.seek(0)
                    # Upload the batch file to S3
                    s3_client.upload_fileobj(batch_csv_buffer, destination_bucket, batch_key) 
                    print(f'policy extract batch {i} file uploaded to s3 bucket {bucket} after skipping compare policy')
                return "Success"       
        else:
            return "Failed"