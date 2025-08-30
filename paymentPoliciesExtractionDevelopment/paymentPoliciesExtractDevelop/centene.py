import json
import requests
import zipfile
import os
from bs4 import BeautifulSoup
from datetime import datetime,date,timedelta
import re
import boto3
import io
import pandas as pd
import csv
from constants import centene_lob_dict, centene_sl_dict, centene_null_state_mapping, centene_state_map, BUCKET_NAME,lob_sl_mapper,remove_special_chars, COLUMN_PREFIX, add_column_prefix,s3_client
from email_body import email_body, send_email_via_ses
import compare_policies

TIMEDELTA = os.environ['TIMEDELTA'] 

error_links = []
rcode = ''

def update_data(data, state,site_name,line_of_business,type_of_service,pdf_links,download_date,file_name,client_name,site):
    data['state'].append(state)
    data['site_name'].append(site_name),
    data['line_of_business'].append(line_of_business)
    data['policy_type'].append(type_of_service)
    data['pdf_links'].append(pdf_links)  
    data['download_date'].append(download_date)
    data['file_name'].append(file_name)
    data['client_name'].append(client_name)
    data['url'].append(site)


def extract_base_url_files(centene_dict):
    count = 0
    client_name = 'Centene'
    final_data = {}
    for market_place,sites in centene_dict.items():
        
        data = {'state' : [],
                'site_name': [],
                'line_of_business': [],
                'policy_type': [],
                'pdf_links': [],  
                'download_date': [],
                'file_name': [],
                'client_name' : [],
                'url' : []
                }
        for site in sites:
            try:
                response = requests.get(site)
                rcode = response.status_code
                error_links.append((site,rcode))
                response.raise_for_status()  # Raise an exception for non-2xx status codes
                html_content = response.content
                # html_text = response.text
                soup = BeautifulSoup(html_content, "html.parser")
                links = soup.find_all("a")
                visited_urls = set()
                for link in links:
                    res = link.get("href")
                    if res is not None:
                        if res.startswith("/") or res.endswith(".pdf") or res.split('?')[0].endswith(".ashx"):
                            res = "https://" + site.split("/")[2] + res if not res.startswith('http') else res
                            res = res.replace(".ashx", ".pdf") if "PDFs" in res and res.endswith(".ashx") else (res.split('?')[0].replace('.ashx', '.pdf') if "PDFs" in res and not res.endswith(".ashx") else res.replace('.ashx', '.pdf'))
                            res = res.replace('.ashx', '.pdf')
                            if not res.endswith("redirect.html"):
                                if res.endswith(".pdf"):
                                    visited_urls.add(res)
                for link in list(visited_urls):
                    if site.split("/")[2] == "www.wellcare.com":
                        if market_place == 'centene_wellcare_payment':
                            update_data(data,site.split("/")[-5],site.split("/")[2],lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict),link,date.today(),link.split('/')[-1],client_name,site)
                        elif market_place == 'centene_wellcare_reimbursement':
                            update_data(data,site.split("/")[-4],site.split("/")[2],lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict),link,date.today(),link.split('/')[-1],client_name,site)
                        else:
                            update_data(data,site.split("/")[-4],site.split("/")[2],lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict),link,date.today(),link.split('/')[-1],client_name,site)
                        
                    elif site.split("/")[2] == 'www.policies-wellcare.com':
                        update_data(data,centene_state_map[site.split("/")[2]],site.split("/")[2],lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict),link,date.today(),link.split('/')[-1],client_name,site)
                    else:
                        for key in centene_state_map:
                            if key == site.split("/")[2]:
                                update_data(data, centene_state_map[key], key,lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict), link, date.today(), link.split('/')[-1], client_name,site)
            except:
                error_links.append((site,rcode))
                update_data(data, 'error', 'error', lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict) , link, date.today(), link.split('/')[-1], client_name,site)
                count+=1
        print(len(data))
        final_data[market_place] = data
    df2 = pd.DataFrame()
    for k,v in final_data.items():
        df1 = pd.DataFrame(v)
        df1['market_place'] = k
        df2 = pd.concat([df1,df2])
    df2.drop_duplicates()
    print(f'Error links {count}')
    return df2

def save_policy_files_to_s3(centene_dict):
    
    bucket_name = BUCKET_NAME
    download_date = date.today()
    df = extract_base_url_files(centene_dict)
    df = df[df['state']!='error']
    client_name = df['client_name'].unique()[0]
    folder_name = df['market_place'].unique()[0]
    download_date = str(df['download_date'].unique()[0])
    df['status'] = 'no change'
    df['effective_date'] = 'NA'

    df.loc[df['state'].isnull(), 'state'] = df.loc[df['state'].isnull(), 'pdf_links'].apply(lambda x: centene_null_state_mapping.get(x.split('/')[2], 'NA'))
    df.loc[pd.isna(df['state']) & df['url'].apply(lambda x: x.split('/')[2] in centene_null_state_mapping.keys()), 'state'] = df.loc[pd.isna(df['state']) & df['url'].apply(lambda x: x.split('/')[2] in centene_null_state_mapping.keys()), 'url'].apply(lambda x: centene_null_state_mapping.get(x.split('/')[2], 'NA'))

    df.columns = add_column_prefix(df, COLUMN_PREFIX)
    df = df[['extrctn_client_name','extrctn_site_name','extrctn_url','extrctn_pdf_links','extrctn_file_name','extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_policy_type','extrctn_download_date','extrctn_status']]

    # Create a csv file
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    out_filename = f'PaymentPolicies/{client_name}/{download_date}/{download_date}_{client_name}_payment_policies.csv'
    s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename) 
    print('policy extract file uploaded to s3')

    error_filename = f'PaymentPolicies/{client_name}/{download_date}/base_url_status_logs/{download_date}_{client_name}_base_url_status_payment_policies.csv'
    error_buffer = io.BytesIO()
    error_df = pd.DataFrame(error_links,columns = ['site','error_code'])
    error_df.to_csv(error_buffer, index=False)
    error_buffer.seek(0)
    s3_client.upload_fileobj(error_buffer, bucket_name, error_filename)
    print('error files uploaded to s3')
    
    download_date =  date.today()
    last_download_date =  str(date.today() - timedelta(days=int(TIMEDELTA)))

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    out_filename = f'PaymentPolicies/{client_name}/{download_date}/{download_date}_{client_name}_payment_policies.csv'
    s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename)

    k1 = f'PaymentPolicies/{client_name}/{last_download_date}/{last_download_date}_{client_name}_payment_policies.csv'
    try:
        response1 = s3_client.get_object(Bucket=bucket_name, Key=k1)
    except Exception as e:
        print(e)
        response1 = {'ResponseMetadata': {'HTTPStatusCode': 404}}
        
    if response1['ResponseMetadata']['HTTPStatusCode'] == 200:
        compare_policies.check_updated_policy(response1, client_name,df,download_date,bucket_name,s3_client)
    else:
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        out_filename = f'PaymentPolicies/{client_name}/{download_date}/{download_date}_{client_name}_payment_policies.csv'
        s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename)

        html_body = email_body(client_name, bucket_name, download_date, df, error_df)
        html_body = html_body.replace('start',"<html><body>").replace('end',"</body></html>")
        send_email_via_ses(f'{client_name} - Policy Update - {download_date}',html_body,sender_email='suriya@zignaai.com',recipient_email=['suriya@zignaai.com','netra@zignaai.com'])

        batch_size = 1000
        batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
        # Store each batch in S3
        destination_bucket = bucket_name
        for i, batch in enumerate(batches):
            batch_key = f'PaymentPolicies/{client_name}/{download_date}/batch/{i}_{client_name}_batch_polices_download.csv'  # Define a key for each batch in S3
            # Create a csv file
            batch_csv_buffer = io.BytesIO()
            batch.to_csv(batch_csv_buffer, index=False)
            batch_csv_buffer.seek(0)
            # Upload the batch file to S3
            s3_client.upload_fileobj(batch_csv_buffer, destination_bucket, batch_key)

