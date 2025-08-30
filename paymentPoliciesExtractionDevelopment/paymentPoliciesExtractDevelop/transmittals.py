import os
from itertools import zip_longest
import re
import io
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime,date,timedelta
from constants import cms_trans_pub_base_url,  COLUMN_PREFIX,add_column_prefix,s3_client, TIMEDELTA
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7
import compare_policies
from email_body import email_body, send_email_via_ses

PAYOR_NAME_DEST_FOLDER_LEVEL_3 = 'cms' 
client_name = 'cms'

def extract_webpage(range_limit, web_element):
    df = pd.DataFrame()
    for i in range(range_limit):
        site = f"{web_element}{i}"
        temp = {
            "url": site,
            "pdf_links": []
        }
        per_page = []
        response = requests.get(site)
        html_content = response.content
        soup = BeautifulSoup(html_content, 'html.parser')
        if soup.find_all("tr"):
            for webpages in soup.find_all("tr"):
                link = webpages.find_all("a", href=True)
                for li in link:
                    if (li["href"].lower().startswith("?") is False) and (li["href"].lower().startswith("http") is False):
                        per_page.append("https://www.cms.gov" + li["href"])
                    elif li["href"].lower().startswith("http"):
                        per_page.append(li["href"])
            per_page_unique = list(set(per_page))
            temp['pdf_links'] = per_page_unique
            df = pd.concat([df, pd.DataFrame(temp)])
            print('number of unique webpages in each page: ', len(set(per_page)))
        else:
            print(f"Page {i} does not contain any policy webpages in the table.")
            break
    return df

def save_policy_files_to_s3(bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        for limit, element, col_name in cms_trans_pub_base_url:
            final_df = extract_webpage(limit, element)
            unique_policy_webpages = list(set(final_df['pdf_links']))
            print("total webpages are: ", len(final_df['pdf_links']))
            print("unique webpages are: ", len(unique_policy_webpages))
            final_df = final_df.drop_duplicates(subset=['pdf_links'])
            final_df['client_name'] = col_name

            final_df['status'] = 'no change'
            download_date =  date.today()
            final_df.columns = add_column_prefix(final_df, COLUMN_PREFIX)

            # Create a csv file
            csv_buffer = io.BytesIO()
            final_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{col_name}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
            s3_client.upload_fileobj(csv_buffer, bucket, out_filename) 
            print(f'webpages extract file uploaded to s3 bucket {bucket} before compare policy')

            LAST_DOWNLOAD_DATE =  str(date.today() - timedelta(days=int(TIMEDELTA)))
            k1 = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{LAST_DOWNLOAD_DATE}/{DEST_FOLDER_LEVEL_6}/{col_name}_{LAST_DOWNLOAD_DATE}_policy_extraction.csv'
            try:
                response1 = s3_client.get_object(Bucket=bucket, Key=k1)
            except Exception as e:
                print(e)
                response1 = {'ResponseMetadata': {'HTTPStatusCode': 404}}
                
            if response1['ResponseMetadata']['HTTPStatusCode'] == 200:
                compare_policies.check_updated_policy(response1, PAYOR_NAME_DEST_FOLDER_LEVEL_3,col_name,final_df,download_date,bucket,s3_client, out_filename)
            else:
                html_body = email_body(col_name, bucket, download_date, final_df, '', out_filename)
                html_body = html_body.replace('start',"<html><body>").replace('end',"</body></html>")
                send_email_via_ses(f'{col_name.upper()} - Policy Update - {download_date}',
                                    html_body,
                                    sender_email='rightpx@zignaai.com',recipient_email=['suriya@zignaai.com','netra@zignaai.com', 'gopireddy@zignaai.com', 'connie@zignaai.com', 'sarvesh@zignaai.com'])        
                batch_size = 100
                batches = [final_df[i:i+batch_size] for i in range(0, len(final_df), batch_size)]
                destination_bucket = bucket
                for i, batch in enumerate(batches):
                    batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_7}/{i}_{col_name}_{DEST_FOLDER_LEVEL_5}_batch_policies_download.csv'
                    batch_csv_buffer = io.BytesIO()
                    batch.to_csv(batch_csv_buffer, index=False)
                    batch_csv_buffer.seek(0)
                    s3_client.upload_fileobj(batch_csv_buffer, destination_bucket, batch_key)
                    print(f'policy extract batch {i} file uploaded to s3 bucket {bucket} after skipping compare policy') 
                    print("Success")