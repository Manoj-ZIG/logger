import requests
import os
from itertools import zip_longest
from bs4 import BeautifulSoup
from datetime import date,timedelta
import re
from constants import COLUMN_PREFIX, add_column_prefix,s3_client,TIMEDELTA
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7
import compare_policies
import io
import pandas as pd
import ast
import json
from email_body import email_body, send_email_via_ses

PAYOR_NAME_DEST_FOLDER_LEVEL_3  = 'uhc' 
client_name = 'uhc'
error_links = []

def categorize_policy(pdf_link):
    try:
        category = pdf_link.split("/")[-1].split(".")[0].split("-")[-3].lower()
    except IndexError:
        if 'clinical-guidelines' in [link_element.lower() for link_element in pdf_link.split('/')]:
            return 'clinical guideline'
        else:
            return 'policy'
    
    if category == 'bulletin':
        return 'bulletin'
    elif category == 'rpub':
        return 'rpub'
    elif category == 'mpub':
        return 'mpub'
    else:
        return 'policy'

def extract_description_and_last_published(site):
    
    description = []
    last_published = []
    pdf_links = []

    response = requests.get(site)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    if site.split("/")[2] == 'www.geha.com':
        script_tag = soup.find_all("script", attrs={'type': 'text/javascript'})
        for script in script_tag:
            script_content = script.string.strip()
            try:
                initialItems = re.search(r"(var initialItems = )('\[.*\]')", str(script_content)).group(2)
                initialItems1 = ast.literal_eval(initialItems)
                initialItems1 = json.loads(initialItems1)
                pdf_links_dic = [item for item in initialItems1 if (item["LinkToResource"].endswith(".pdf")) and (item["LinkToResource"].split("/")[-2]=='coverage-policies')]
                for pdf_dic in pdf_links_dic:
                    pdf_links.append("https://" + site.split("/")[2] + pdf_dic['LinkToResource'])
                    description.append(pdf_dic['Description'])
                    dates = re.findall(r"(?<!\d)\d{4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{1,2}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\d{1,2}[-./\, _:|]\d{1,2}[-./\, _:|]\d{4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\d{1,4}[-./\, _:|]\d{1,4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?(?!\d)",
                                pdf_dic['Description']) or re.findall(r"(?<!\d)\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b(?!\d)",
                                pdf_dic['Description']) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b(?!\d)",
                                pdf_dic['Description']) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?!\d)", pdf_dic['Description']) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b(?!\d)", pdf_dic['Description'])
                    pub_date = dates[-1]
                    last_published.append(pub_date)
            except:
                continue

    for link_tag, desc_tag in [("faceted-list-item", "faceted-item-description"), ("elementor-widget-container", " ")]:
        for div in soup.find_all("div", class_= link_tag):
            try:
                link = div.find_all("a", href=True)
                for li in link:
                    if li["href"].endswith(".pdf"):
                        if site.split("/")[2] == 'www.rmhp.org':
                            pdf_links.append(li["href"])
                        else:
                            pdf_links.append("https://" + site.split("/")[2] + li["href"])
                last_publish = div.find("p")
                if last_publish:
                    last_published.append((last_publish.text).split(" ")[-1])
                    
                else:
                    last_published.append("NA")
                para = div.find_all("p", class_= desc_tag)
                for p in para:
                    if p:
                        description.append(p.text)
                    else:
                        description.append("NA")
            except:
                continue
    for link_tag, desc_tag in [("faceted-list-item-filterable", "faceted-item-description"), ("botom-margin", "cmp-text__text cmp-pdf_list")]:
        for li in soup.find_all("li", class_= link_tag):
            try:
                link = li.find("a")["href"]
                pdf_links.append("https://" + site.split("/")[2] + link)
                last_publish = li.find("p")
                if last_publish:
                    last_published.append((last_publish.text).split(" ")[2])
                else:
                    last_published.append("NA")
                para = li.find_all("p", class_= desc_tag)
                for p in para:
                    if p:
                        description.append(p.text)
                    else:
                        description.append("NA")
            except:
                continue
    return pdf_links, description, last_published

# Create function to extract html content
def extract_html_content(site):
    
    site_name = "https://" + site.split("/")[2]
    policy_type = site.split('/')[-2]
    line_of_business = site.split('/')[-1].split('.')[0]
    line_of_business = re.sub(r'-policies', '', line_of_business) if re.search(r'dental', line_of_business) else line_of_business
    temp = {'site_name':site_name,
            'url':'',
            'affiliate':'',
            'line_of_business':'',
            'state':'',
            'policy_type':policy_type,
            'pdf_links' : [],
            'description' : [],
            'last_published' : []
           }
    response = requests.get(site)
    html_content = response.content
    html_text = response.text
    
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a')
    return temp,links,response.status_code

# Create function to extract base urls
def extract_base_url_files(affiliate, url_lob_state):
    l1 = []
    downloadable_links = []
    ex_lob = set()
    for site in url_lob_state.keys():
        temp,links,rcode = extract_html_content(site)
        temp['url'] = site
        temp['affiliate'] = affiliate
        error_links.append((site,rcode))
        site_name = temp['site_name']
        temp['line_of_business'] = url_lob_state[site]['lob']
        temp['state'] = url_lob_state[site]['state']
        policy_type = temp['policy_type']
               
        pdf_links, description, last_published = extract_description_and_last_published(site)
        if (len(pdf_links) == len(description)) and (len(last_published) == len(description)):
            for i in zip(pdf_links, description, last_published):
                temp['pdf_links'].append(i[0])
                temp['description'].append(i[1])
                temp['last_published'].append(i[2])
        else:
            for i in list(zip_longest(pdf_links, description, last_published)):
                temp['pdf_links'].append(i[0])
                temp['description'].append(i[1])
                temp['last_published'].append(i[2])
            
        if len(temp['pdf_links']) != 0:
            downloadable_links.append(temp)
    return downloadable_links

def save_policy_files_to_s3(uhc_base_urls, bucket, api):
    print(bucket, api)
    final_df = pd.DataFrame()
    if (bucket != '') and (api != ''):
        download_date =  date.today()

        for key, base_urls_lob_state in uhc_base_urls.items():
            affiliate_urls_list = base_urls_lob_state.keys()
            print("affiliate: ", key)
            downloadable_links = extract_base_url_files(key, base_urls_lob_state) 

            df = pd.DataFrame(downloadable_links)
            print("df shape is: ", df.shape)
            print("df columns are: ", df.columns)
            if len(df) != 0:
                payor_df= df.explode(['pdf_links', 'description', 'last_published'])
                payor_df.rename(columns = {'last_published':'effective_date'}, inplace = True)
                payor_df = payor_df.dropna(subset=['pdf_links'])
                print(payor_df.shape)
                payor_df['state'] = payor_df['state'].astype('string') 
                payor_df['line_of_business'] = payor_df['line_of_business'].astype('string')
                payor_df = payor_df.drop_duplicates()
                print(payor_df.shape)
                final_df = pd.concat([final_df, payor_df])
            else:
                continue

        final_df['download_date'] = download_date
        final_df['client_name'] = client_name
        final_df['file_name'] = final_df['pdf_links'].apply(lambda x : x.split('/')[-1])
        final_df = final_df[~(final_df['pdf_links'].str.contains('/dental/', case=False, na=False))]
        final_df['status'] = 'no change'
        final_df['policy_category'] = final_df['pdf_links'].apply(categorize_policy)
        final_df.columns = add_column_prefix(final_df, COLUMN_PREFIX)
        print(final_df.shape)
        final_df = final_df.drop_duplicates()							
        final_df = final_df[['extrctn_client_name', 'extrctn_affiliate','extrctn_site_name','extrctn_url','extrctn_pdf_links','extrctn_file_name', 'extrctn_policy_category', 'extrctn_effective_date','extrctn_state','extrctn_line_of_business','extrctn_download_date','extrctn_status']]
        print('raw file generated')
        print(final_df.shape)
        print('raw downloaded to local')

        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False)
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
            compare_policies.check_updated_policy(response1, PAYOR_NAME_DEST_FOLDER_LEVEL_3, PAYOR_NAME_DEST_FOLDER_LEVEL_3,final_df,download_date,bucket,s3_client, out_filename)
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

            batch_size = 100
            dedup_file = final_df[['extrctn_client_name', 'extrctn_pdf_links']]
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
            print("Success", final_df.shape)
        return final_df