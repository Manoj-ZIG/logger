import os
from itertools import zip_longest
import io
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime,date,timedelta
from constants import s3_client
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_8,DEST_FOLDER_LEVEL_9,DEST_FOLDER_LEVEL_10
import re

pdf_links_all_trans = []
pdf_links_all_pub = []

PAYOR_NAME_DEST_FOLDER_LEVEL_3 = 'cms'
client_name = 'cms'

def extract_metadata_pdflink_transmittals(webpage):
    df_dict = {'extrctn_client_name':webpage['extrctn_client_name'],
            'base_url': webpage['extrctn_url'],
            'webpage_url': webpage['extrctn_pdf_links'],
            'extrctn_status': webpage['extrctn_status'],
            'pdf_links': [],
            'Transmittal #': "",
            'Issue Date': "",
            'Subject' : "",
            'Implementation Date': "",
            'CR #': "",
            "Provider Education Release Date": "",
            "Provider Education Revised Date":""
            }
    response = requests.get(webpage['extrctn_pdf_links'])
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    if soup.find_all("li", class_ = "field__item"):
        for links in soup.find_all("li", class_ = "field__item"):
            pdflinks = links.find_all("a", href=True)
            for li in pdflinks:
                if (li['href'].lower().endswith(".pdf")) and (li["href"].lower().startswith("http") is False):
                    pdf_links_all_trans.append("https://www.cms.gov" + li['href'])
                    df_dict['pdf_links'].append("https://www.cms.gov" + li['href'])
                elif (li['href'].lower().endswith(".pdf")) and  (li["href"].lower().startswith("http")):
                    pdf_links_all_trans.append(li['href'])
                    df_dict['pdf_links'].append(li['href'])
    else:
        df_dict['pdf_links'].append('NA')
    for attribute in list(df_dict.keys())[5:]:
        try:
            label_div = soup.find('div', class_='field__label', string=attribute)
            if attribute != 'Subject':
                df_dict[attribute] = label_div.find_next('div', class_='field__item').text
            elif attribute == 'Subject':
                df_dict[attribute] = (label_div.find_next('div', class_='field__item')).find('p').text
        except Exception as e:
            print('The error is: ', e)
            df_dict[attribute] = 'NA'
    attribute_df = pd.DataFrame(df_dict)
    return attribute_df

def remove_related_links(soup):
    related_links_section = soup.find_all('h2', class_='field__label block-title-icon')
    if related_links_section:
        for link in related_links_section:
            if link.text == 'Related Links':
                related_links_section = link.find_parent()
                if related_links_section:
                    related_links_section.decompose()
    return soup

def extract_metadata_pdflink_publications(webpage):
    df_dict = {
            'extrctn_client_name':webpage['extrctn_client_name'],
            'base_url': webpage['extrctn_url'],
            'webpage_url': webpage['extrctn_pdf_links'],
            'extrctn_status': webpage['extrctn_status'],
            'pdf_links': [],
            'Date': "",
            'Topic': "",
            'Title' : "",
            'Format': "",
            'ICN': ""
            }
    response = requests.get(webpage['extrctn_pdf_links'])
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    cleaned_soup = remove_related_links(soup)
    tag_attr_list = [("li", "field__item"), ("div", "field field--name-body field--type-text-with-summary field--label-hidden field__item")]
    for tag, attr in tag_attr_list:
        if cleaned_soup.find_all(tag, class_ = attr):
            for links in cleaned_soup.find_all(tag, class_ = attr):
                pdflinks = links.find_all("a", href=True)
                for li in pdflinks:
                    if ((".pdf" in li['href'].lower()) or (li['href'].lower().startswith('/mln'))) and (li["href"].lower().startswith("http") is False):
                        pdf_links_all_pub.append("https://www.cms.gov" + li['href'])
                        df_dict['pdf_links'].append("https://www.cms.gov" + li['href'])
                    elif ((".pdf" in li['href'].lower()) or (li['href'].lower().startswith('/mln'))) and (li["href"].lower().startswith("http")):
                        pdf_links_all_pub.append(li['href'])
                        df_dict['pdf_links'].append(li['href'])
        if df_dict['pdf_links'] != []:
            break
    if df_dict['pdf_links'] == []:
        df_dict['pdf_links'].append('NA')
    for attribute in list(df_dict.keys())[5:]:
        try:
            if attribute != 'ICN':
                label_div = soup.find('div', class_='field__label', string=attribute)
                df_dict[attribute] = label_div.find_next('div', class_='field__item').text
            elif attribute == 'ICN':
                label_div = soup.find_all('div', class_='field field--name-body field--type-text-with-summary field--label-hidden field__item')
                for label in label_div:
                    if 'ICN' in label.find('p').text:
                        p_text = re.sub(r'<br/>.*?</p>', '', str(label.find('p')), flags=re.DOTALL)
                        p_tag = BeautifulSoup(p_text, 'html.parser')
                        df_dict[attribute] = re.sub(r'(\d+)[^\d].*', r'\1', (p_tag.text).replace('ICN:', '').replace('ICN', '').strip())
                        break
        except Exception as e:
            print('The error is: ', e)
            df_dict[attribute] = 'NA'
    attribute_df = pd.DataFrame(df_dict)
    return attribute_df

def update_status(df,pdf_link, s3_path, status_code,file_name, bucket):
    
    s3_link = f"https://{bucket}.s3.amazonaws.com/{s3_path}"
    s3_path = f's3://{bucket}/{s3_path}'
    df.loc[df['pdf_links'] == pdf_link, 'dwnld_s3_uri'] = s3_path
    df.loc[df['pdf_links'] == pdf_link, 'dwnld_s3_url'] = s3_link
    df.loc[df['pdf_links'] == pdf_link, 'dwnld_status_code'] = status_code
    df.loc[df['pdf_links'] == pdf_link, 'dwnld_file_name'] = file_name

def download_files(df,batch, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        sub_type = str(df['extrctn_client_name'][0]).lower()
        print('sub type is ', sub_type)
        final_df = pd.DataFrame()

        if sub_type == 'transmittals_and_mln_articles':
            for index, wp in df.iterrows():
                print(index, wp['extrctn_pdf_links'])
                attributedf = extract_metadata_pdflink_transmittals(wp)
                final_df = pd.concat([final_df, attributedf])
            print('finaldf shape is: ', final_df.shape)
        elif sub_type == 'publications':
            for index, wp in df.iterrows():
                print(index, wp['extrctn_pdf_links'])
                attributedf = extract_metadata_pdflink_publications(wp)
                final_df = pd.concat([final_df, attributedf])
            print('finaldf shape is: ', final_df.shape)

        downloadable_links = list(set(final_df['pdf_links']))
        for link in downloadable_links:
            try:
                response = requests.get(link)
                if response.status_code == 200:
                    fname = link.split('/')[-1].replace('.pdf','').replace('%20','_') 
                    pattern = re.compile('[^a-zA-Z0-9]+')
                    file_name = re.sub(pattern, '_', fname) + '.pdf'
                    modified_filename = f"{sub_type}_{file_name}"
                    s3_path = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_8}/{modified_filename}'
                    print('s3 path is ', s3_path)
                    s3_client.put_object(Bucket=bucket, Key=s3_path, Body=response.content, ContentType='application/pdf')
                    update_status(final_df, link,s3_path, response.status_code,modified_filename, bucket)
                    print(f'Success: policy downloaded to bucket {bucket}')
                else:
                    update_status(final_df, link, 'NA', response.status_code,"NA", bucket)
                    print(f'Response not 200 - {response.status_code}')
            except Exception as e:
                    update_status(final_df, link, 'NA', response.status_code,"NA", bucket)
                    print(f'Response or logic Failed - {e}')
        
        # Create a csv file
        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_9}/{batch}_{sub_type}_{DEST_FOLDER_LEVEL_5}_policy_download_status.csv'
        print('outfile name is ', out_filename)
        s3_client.upload_fileobj(csv_buffer, bucket, out_filename) 
        print(f'policy extract file uploaded to s3 bucket {bucket}')

        batch_size = 100
        batches = [final_df[i:i+batch_size] for i in range(0, len(final_df), batch_size)]
        # Store each batch in S3
        for i, btch in enumerate(batches):
            batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_10}/{batch}_{i}_{sub_type}_{DEST_FOLDER_LEVEL_5}_policy_abstract_trigger.csv'
            batch_csv_buffer = io.BytesIO()
            btch.to_csv(batch_csv_buffer, index=False)
            batch_csv_buffer.seek(0)
            s3_client.upload_fileobj(batch_csv_buffer, bucket, batch_key)
            print(f"Abstract trigger batch {i} file uploaded to bucket {bucket}")
        return "Files uploaded to S3 bucket"