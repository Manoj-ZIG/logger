from bs4 import BeautifulSoup
import requests
from datetime import datetime,date,timedelta
from constants import devoted_base_url, COLUMN_PREFIX,add_column_prefix,s3_client, TIMEDELTA,webpage_devoted,devoted_states_list
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7
import pandas as pd
import os
import io
import compare_policies
from email_body import email_body, send_email_via_ses

def get_states(webpage):
    devoted_states_list = []
    response = requests.get(webpage)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup.find_all("h2"):
        devoted_states_list.append(tag.text)
    return devoted_states_list

error_links = []
PAYOR_NAME_DEST_FOLDER_LEVEL_3 = 'devoted'
client_name = 'devoted'
download_date =  date.today()

def extract_html_content(site):

    '''
    extract html content from the site
    args:
        site: url of the site
    return:
        soup: html content of the site
        status_code: status code of the site
    '''
    response = requests.get(site)
    response.raise_for_status()
    html_content = response.content
    html_text = response.text
    soup = BeautifulSoup(html_text, 'html.parser')
    return soup, response.status_code

def update_data(data, link, name, client, site, url, eff_date, state, line_of_business, policy_type, downloaddate, status):
    data['pdf_links'].append(link)
    data['file_name'].append(name)
    data['client_name'].append(client)
    data['site_name'].append(site)
    data['url'].append(url)
    data['effective_date'].append(eff_date)
    data['state'].append(state)
    data['line_of_business'].append(line_of_business)
    data['policy_type'].append(policy_type)
    data['download_date'].append(download_date)
    data['status'].append(status)

def extract_pdf_links_and_description(site_name):

    '''
    extracts the pdf links and its description from the site
    args:
        site: url of the site
    return:
        final extraction dataframe
    '''
    df_dict = {'pdf_links': [],
               'file_name': [],
               'client_name': [],
               'site_name' : [],
               'url': [],
               'effective_date': [],
               'state': [],
               'line_of_business': [],
               'policy_type': [],
               'download_date': [],
               'status': []
               }
    for site in site_name:
        soup, rcode = extract_html_content(site)
        pdf_links = soup.select('a[href^="https://"]:not([href$=".com/"]):not([class])')
        for link in pdf_links:
            try:
                # states_list = get_states(webpage_devoted)
                states_list = devoted_states_list
                update_data(df_dict, link['href'], link.string, 'devoted', site.split('/')[2], site, 'NA', states_list, 'Medicare Advantage', 'Payment policy', download_date, 'NA')
            except:
                error_links.append((site,rcode))
                update_data(df_dict, 'error', 'error', 'devoted', site.split('/')[2], site, 'NA', 'NA', 'Medicare Advantage', 'Payment policy', download_date, 'error')

    df = pd.DataFrame(df_dict)
    return df

def save_policy_files_to_s3(site, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        final_df = extract_pdf_links_and_description(site)
        final_df = final_df[final_df['status']!='error']
        final_df['status'] = 'no change'
        download_date =  date.today()
        final_df.columns = add_column_prefix(final_df, COLUMN_PREFIX)
        final_df = final_df[['extrctn_client_name','extrctn_site_name','extrctn_url','extrctn_pdf_links','extrctn_file_name','extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_policy_type','extrctn_download_date','extrctn_status']]

        # Create a csv file
        csv_buffer = io.BytesIO()
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

            batch_size = 1000
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
            print('Success')
        return final_df