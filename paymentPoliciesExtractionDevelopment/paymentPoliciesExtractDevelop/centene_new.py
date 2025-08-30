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
from constants import centene_lob_dict, centene_sl_dict, centene_null_state_mapping, centene_state_map,lob_sl_mapper,remove_special_chars, COLUMN_PREFIX, add_column_prefix,s3_client,TIMEDELTA
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7
from email_body import email_body, send_email_via_ses
import compare_policies

# TIMEDELTA = os.environ['TIMEDELTA'] 
PAYOR_NAME_DEST_FOLDER_LEVEL_3  = 'centene' 
client_name = 'centene'
error_links = []
rcode = ''

def update_data(data,site_name,line_of_business,type_of_service,pdf_links,download_date,file_name,client_name,site,affi):
    data['site_name'].append(site_name),
    data['line_of_business'].append(line_of_business)
    data['policy_type'].append(type_of_service)
    data['pdf_links'].append(pdf_links)  
    data['download_date'].append(download_date)
    data['file_name'].append(file_name)
    data['client_name'].append(client_name)
    data['url'].append(site)
    data['affiliate'].append(affi)


def extract_base_url_files(affi, centene_affi_urls):
    final_df = pd.DataFrame()
    count = 0
    for site in list(set(centene_affi_urls)):
        print(site)
        data = {
                'site_name': [],
                'line_of_business': [],
                'policy_type': [],
                'pdf_links': [],  
                'download_date': [],
                'file_name': [],
                'client_name' : [],
                'url' : [],
                'affiliate': []
                }
        try:
            response = requests.get(site)
            rcode = response.status_code
            print(rcode)
            # error_links.append((site,rcode))
            response.raise_for_status() 
            html_content = response.content
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
                update_data(data,site.split("/")[2],lob_sl_mapper(link,centene_lob_dict),lob_sl_mapper(link,centene_sl_dict),link,date.today(),link.split('/')[-1],client_name,site,affi)
        except:
            error_links.append((site,rcode))
            update_data(data, 'error', 'NA', 'NA', 'NA', date.today(), 'NA', client_name,site,affi)
            count+=1
        print('data lengt: ', (pd.DataFrame(data)).shape)
        final_df = pd.concat([final_df, pd.DataFrame(data)])
    return final_df

def save_policy_files_to_s3(centene_dict_new, bucket, api):
    print(bucket, api)
    main_df = pd.DataFrame()
    if (bucket != '') and (api != ''):
        download_date =  date.today()
        for key, base_urls in centene_dict_new.items():
            print("affiliate: ", key)
            affiliate_df = extract_base_url_files(key, base_urls)
            if len(affiliate_df) != 0:
                affiliate_df = affiliate_df.dropna(subset=['pdf_links'])
                print(affiliate_df.shape)
                affiliate_df = affiliate_df.drop_duplicates()
                print(affiliate_df.shape)
                main_df = pd.concat([main_df, affiliate_df])
            else:
                continue
        main_df = main_df[main_df['site_name']!='error']
        main_df['status'] = 'no change'

        main_df.columns = add_column_prefix(main_df, COLUMN_PREFIX)
        print(main_df.shape)
        main_df = main_df.drop_duplicates()							
        # main_df = main_df[['extrctn_client_name', 'extrctn_affiliate','extrctn_site_name','extrctn_url','extrctn_pdf_links','extrctn_file_name', 'extrctn_policy_category', 'extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_download_date','extrctn_status']]
        print('raw file generated')
        print(main_df.shape)
        # print('raw downloaded to local')

        csv_buffer = io.BytesIO()
        main_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
        s3_client.upload_fileobj(csv_buffer, bucket, out_filename)
        print(f'policy extract file uploaded to s3 bucket {bucket} before comparing policies')

        error_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction_base_url_logs.csv'
        error_buffer = io.BytesIO()
        error_df = pd.DataFrame(error_links,columns = ['site','error_code'])
        error_df.to_csv(error_buffer, index=False)
        error_buffer.seek(0)
        s3_client.upload_fileobj(error_buffer, bucket, error_filename)
        print(f'error files uploaded to s3 bucket {bucket} before comparing policies')

        download_date =  date.today()
        LAST_DOWNLOAD_DATE =  str(date.today() - timedelta(days=int(TIMEDELTA)))

        k1 = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{LAST_DOWNLOAD_DATE}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{LAST_DOWNLOAD_DATE}_policy_extraction.csv'
        try:
            response1 = s3_client.get_object(Bucket=bucket, Key=k1)
        except Exception as e:
            print(e)
            response1 = {'ResponseMetadata': {'HTTPStatusCode': 404}}
            
        if response1['ResponseMetadata']['HTTPStatusCode'] == 200:
            compare_policies.check_updated_policy(response1, PAYOR_NAME_DEST_FOLDER_LEVEL_3, PAYOR_NAME_DEST_FOLDER_LEVEL_3,main_df,download_date,bucket,s3_client, out_filename)
        else:
            csv_buffer = io.BytesIO()
            main_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
            s3_client.upload_fileobj(csv_buffer, bucket, out_filename)
            print(f'policy extract file uploaded to s3 bucket {bucket} after skipping compare policy')

            html_body = email_body(PAYOR_NAME_DEST_FOLDER_LEVEL_3, bucket, download_date, main_df, error_df, out_filename)
            html_body = html_body.replace('start',"<html><body>").replace('end',"</body></html>")
            send_email_via_ses(f'{PAYOR_NAME_DEST_FOLDER_LEVEL_3.upper()} - Policy Update - {download_date}',
                            html_body,
                            sender_email='rightpx@zignaai.com',recipient_email=['netra@zignaai.com'])

            batch_size = 1000
            dedup_file = main_df[['extrctn_client_name', 'extrctn_pdf_links']]
            dedup_file = dedup_file.drop_duplicates()

            batches = [dedup_file[i:i+batch_size] for i in range(0, len(dedup_file), batch_size)]
            destination_bucket = bucket
            for i, batch in enumerate(batches):
                batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_7}/{i}_{PAYOR_NAME_DEST_FOLDER_LEVEL_3}_{DEST_FOLDER_LEVEL_5}_batch_policies_download.csv'
                batch_csv_buffer = io.BytesIO()
                batch.to_csv(batch_csv_buffer, index=False)
                batch_csv_buffer.seek(0)
                s3_client.upload_fileobj(batch_csv_buffer, destination_bucket, batch_key)
                print(f'policy extract batch {i} file uploaded to s3 bucket {bucket} after skipping compare policy') 
            print("Success", main_df.shape)
        return main_df