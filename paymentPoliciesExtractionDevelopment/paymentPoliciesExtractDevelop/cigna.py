import urllib.parse
import json
from constants import cigna_dict,BUCKET_NAME,lob_sl_mapper,remove_special_chars,COLUMN_PREFIX, add_column_prefix, cigna_sl_dict,s3_client
import boto3
from bs4 import BeautifulSoup
import requests
from datetime import datetime,date,timedelta
import pandas as pd
import os
import io
from email_body import email_body, send_email_via_ses
import compare_policies

TIMEDELTA = os.environ['TIMEDELTA'] 
error_links = []
download_date =  date.today()
client_name = 'Cigna'


def extract_html_content(site):

    response = requests.get(site)
    response.raise_for_status()
    html_content = response.content
    html_text = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    return soup,response.status_code

def extract_base_url_files(cigna_dict):

    pdf_details_list = []
    pdf_links_list_evicore = []
    dataframes = []
    import pandas as pd
    df1 = pd.DataFrame()
    for key,site in cigna_dict.items():
        if  site != 'https://www.evicore.com/cignamedicare' and 'policyUpdates' not in site:
            soup,rcode = extract_html_content(site)
            error_links.append((site,rcode))
            table = soup.find('table')
            rows = []
            for row in table.find_all('tr'):
                pdf_details_list1 = []
                try:
                    for cell in row.find_all(['td', 'th']):
                        pdf_details_list1.append(cell.text.strip())
                        if cell.find('a') :
                            pdf_details_list1.append(cell.find('a')['href'])
                            pdf_details_list1.append(urllib.parse.urljoin(site,cell.find('a')['href']))
                    if len(pdf_details_list1) == 6:
                        pdf_details_list1.insert(3,'NA')
                    pdf_details_list1.append(key)
                    pdf_details_list1.append(site)
                    pdf_details_list.append(pdf_details_list1)
                except Exception as e:
                    print(f'Error :{e}')
                    
        elif site == 'https://www.evicore.com/cignamedicare':
            try:
                soup,rcode = extract_html_content(site)
                error_links.append((site,rcode))
                pdf_links = soup.select('a[href$=".pdf"]')
                for link in pdf_links:
                    pdf_links_list_evicore.append([link['href'].split('/')[-1].replace('.pdf',''),'NA',link['href'],'NA','PDF','NA','NA',key,site])
            except Exception as e:
                print(f'Error :{e}')
        elif 'policyUpdates' in site:
            try:
                soup,rcode = extract_html_content(site)
                error_links.append((site,rcode))
                pdf_links = soup.select('a[href$=".pdf"]')
                pdf_links_list = [link['href'] for link in pdf_links]
            except Exception as e:
                print(f'Error :{e}')

    pdf_details_list.extend(pdf_links_list_evicore)        
    error_filename = f'PaymentPolicies/{client_name}/{download_date}/base_url_status_logs/{download_date}_{client_name}_payment_policies.csv'
    error_buffer = io.BytesIO()
    pd.DataFrame(error_links,columns = ['site','error_code']).to_csv(error_buffer, index=False)
    error_buffer.seek(0)
    s3_client.upload_fileobj(error_buffer, BUCKET_NAME, error_filename)

    return pdf_details_list

def save_policy_files_to_s3(cigna_dict):

    client_name = 'Cigna'
    folder_name = 'PaymentPolicies'
    bucket_name = BUCKET_NAME
    download_date =  date.today()

    downloadable_links = extract_base_url_files(cigna_dict) 

    df = pd.DataFrame(downloadable_links).explode(1)
    df = df[df[0]!='Document Title']
    df.columns = ['file_name','dummy_url','pdf_links','code','file_type','size','effective_date','keywords','url']

    df = df.drop_duplicates()
    df['download_date'] = download_date
    df['client_name'] = client_name
    df['status'] = 'no change'
    df['line_of_business'] = 'NA'
    df['state'] = 'NA'

    # fill site_name
    split_term = '.com'
    df['site_name'] = df['url'].apply(lambda x : x.split(split_term)[0] + split_term) 
    # policy type
    df['policy_type'] = df['pdf_links'].apply(lambda x :lob_sl_mapper(x,cigna_sl_dict))

    # Create a csv file
    df.columns = add_column_prefix(df, COLUMN_PREFIX)
    df = df[['extrctn_client_name','extrctn_site_name','extrctn_url','extrctn_pdf_links','extrctn_file_name','extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_policy_type','extrctn_download_date','extrctn_status']]


    download_date =  date.today()
    last_download_date =  str(date.today() - timedelta(days=int(TIMEDELTA)))

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    out_filename = f'PaymentPolicies/{client_name}/{download_date}/{download_date}_{client_name}_payment_policies.csv'
    s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename)


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