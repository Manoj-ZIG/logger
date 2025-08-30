import json
import pandas as pd
import boto3
from io import BytesIO
import io
import os
from pdfminer.high_level import extract_text
from datetime import datetime, date
import time
import numpy as np
from constants import get_auth_token, replace_keys_with_ids, payor_id_mapping_fn, add_column_prefix, all_ids_per_category, document_type_map, affiliate_map, states_map, lineofbusiness_map, service_level_map, contract_type_map, contractor_name_map
from constants import APPLICATION_DEST_FOLDER_LEVEL_2, APPLICATION_DEST_FOLDER_LEVEL_3, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2, RAW_DEST_FOLDER_LEVEL_4, RAW_DEST_FOLDER_LEVEL_5, RAW_DEST_FOLDER_LEVEL_6, RAW_DEST_FOLDER_LEVEL_7, DOWNLOAD_FOLDER
from uhc_attributes_extraction import section_cleanup, section_range, date_formats, extract_text_for_section, dates_from_text, check_effective_date_history
import traceback
import sys
from constants import COLUMN_PREFIX,s3_client, ssm
from decimal import Decimal
import requests
import re

# custom import
from send_payload import send_payload

APPLICATION_DEST_FOLDER_LEVEL_1 = 'cms' 
client_name = 'cms'

def process_files_trans_art(df):
    try:
        df = df.dropna(subset=['pdf_links'])
        df['pdf_links'] = df['pdf_links'].apply(lambda x: x.lower() if isinstance(x,str) else x)
        df['Provider Education Revised Date'] = df['Provider Education Revised Date'].fillna('')
        df['Provider Education Release Date'] = df['Provider Education Release Date'].fillna('')
        df['Transmittal #'] = df['Transmittal #'].astype(str)
        df['CR #'] = df['CR #'].astype(str)

        condition = df['pdf_links'].apply(lambda x: x.split('/')[-1].startswith('r'))
        transmittal_df = df[condition]
        mln_article_df = df[~condition]
        print('transmittals df shape: ', transmittal_df.shape)
        print('mln articles df shape: ', mln_article_df.shape)

        transmittal_df['policy_number'] = transmittal_df['Transmittal #']
        transmittal_df['title'] = transmittal_df['Subject']
        transmittal_df['policy_type'] = 'transmittals'
        transmittal_df['date_from_webpage'] = transmittal_df['Issue Date']

        mln_article_df['policy_number'] = mln_article_df['pdf_links'].apply(lambda x: x.split('/')[-1].split('.')[0].split('-')[0] if str(x.split('/')[-1].split('.')[0]).startswith(('mm', 'se', 'mln', 'ja')) else 'NA')
        mln_article_df['title'] = mln_article_df['pdf_links'].apply(lambda x: " ".join(x.split('/')[-1].split('.')[0].split('-')[1:]) if str(x.split('/')[-1].split('.')[0]).startswith(('mm', 'se', 'mln', 'ja')) else " ".join(x.split('/')[-1].split('.')[0].split('-')))
        mln_article_df['title'] = mln_article_df.apply(lambda row: row['Subject'] if row['title']=='' else row['title'], axis=1)        
        mln_article_df['policy_type'] = 'mln articles'
        mln_article_df['date_from_webpage'] = mln_article_df['Provider Education Revised Date'].where(mln_article_df['Provider Education Revised Date'] != '', mln_article_df['Provider Education Release Date'])

        final_df = pd.concat([transmittal_df, mln_article_df])
        pattern = re.compile('[^a-zA-Z0-9 ]+')
        final_df['title'] = final_df['title'].apply(lambda x: re.sub(pattern, '', x) if pd.notnull(x) else x)
        final_df['line_of_business'] = 'medicare'
        final_df['state'] = 'null'
        final_df['service_level'] = 'null'
        return final_df
    except Exception as e:
        print('The error during transmittals file processing is: ', e)

def process_files_pub(df):
    try:
        df = df[~(df['Format']=='Video')]
        df = df.dropna(subset=['pdf_links'])
        df['pdf_links'] = df['pdf_links'].apply(lambda x: x.lower() if isinstance(x,str) else x)
        df['Date'] = df['Date'].fillna('')
        df['ICN'] = df['ICN'].fillna('')
        df['ICN'] = df['ICN'].astype(str)

        df['policy_number'] = df['pdf_links'].apply(lambda x: x.split('/')[-1].split('.')[0].split('-')[0] if str(x.split('/')[-1].split('.')[0]).startswith('mln') else 'NA')
        df['policy_number'] = df.apply(lambda row: row['ICN'] if row['policy_number'] == 'NA' else row['policy_number'], axis=1)

        df['policy_number'] = df['policy_number'].apply(lambda x: f"mln{x}" if not x.lower().startswith('mln') and x!= '' else x)

        df['title'] = df['pdf_links'].apply(lambda x: " ".join(x.split('/')[-1].split('.')[0].split('-')[1:]) if str(x.split('/')[-1].split('.')[0]).startswith('mln') else 'NA')
        df['title'] = df.apply(lambda row: row['Title'] if row['title'] == 'NA' else row['title'], axis=1)        
        df['policy_type'] = 'mln publications'
        df['effective_date'] = df['Date']

        pattern = re.compile('[^a-zA-Z0-9]+')
        df['title'] = df['title'].apply(lambda x: re.sub(pattern, '', x))
        df['line_of_business'] = 'medicare'
        df['state'] = 'null'
        df['service_level'] = 'null'
        return df
    except Exception as e:
        print('The error during publications file processing is: ', e)

def read_pdf_from_s3(bucket, object_key):
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=object_key)
        pdf_byte = resp['Body'].read()
        text = extract_text(BytesIO(pdf_byte))
        return pdf_byte, text
    except Exception as e:
        print("Error: File not found", e)
        pdf_byte = ''
        text = ''
        return pdf_byte, text
    
def effective_date_regex(text, word):
    effective_date = re.findall(rf"{word}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,2}}[-./, _:|]\d{{2,4}}", text, re.IGNORECASE) or re.findall(rf"{word}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}(?:st|nd|rd|th)?", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?\s+\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?,\s+\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"\b{word}(?::)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}},\s*?\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{{1,2}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{4}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}})\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text, re.IGNORECASE) or re.findall(rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}", text, re.IGNORECASE)
    if effective_date:
        return effective_date[0]
    else:
        return 'NA'

def find_date_at_start(text):
    date_pattern= r"((?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s*?\d{2,4}|\d{1,4}[-./, _:|]\d{1,2}[-./, _:|]\d{2,4}|\d{2}[-./, _:|]\d{1,2}|\d{4}[-./, _:|]\d{1,2}|(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}|\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December))"
    match = re.findall(date_pattern, text, re.I)
    return match[0] if match else 'NA'

def extract_section_text_from_content(text, section_name):
    lines = text.split("\n")
    df = section_cleanup(text)
    df3 = section_range(df)
    if len(df3) != 0:
        if section_name in df3['section'].to_list():
            section_text = extract_text_for_section(df3, section_name, lines)
            sec_text_joined = "\n".join(section_text)
    else:
        sec_text_joined = text
    return sec_text_joined

def get_date_from_text(p_link, text):
    date_from_text = 'NA'
    if p_link.split('/')[-1].startswith('r'):
        line_text = " ".join((text.lower()).split("\n")[:20])
        if 'date:' in line_text.lower().strip():
            effective_date1 = effective_date_regex(line_text, 'date')
            date_from_text = effective_date1.split(
                effective_date1.split(" ")[0])[-1].replace('date:', '').replace('date ', '').replace('date', '').replace(':', '').strip()
    else:
        sec_data = extract_section_text_from_content(text.lower(), 'document history')
        date_from_text = find_date_at_start(sec_data)
        if date_from_text == 'NA':
            line_text = " ".join((text.lower()).split("\n")[:20])
            if 'date posted:' in line_text.lower().strip():
                effective_date1 = effective_date_regex(line_text, 'date posted')
                date_from_text = effective_date1.split(
                effective_date1.split(" ")[0])[-1].replace('posted:', '').replace('posted ', '').replace('posted', '').replace(':', '').strip()
    return date_from_text

def get_title_from_text(pdflink, text):
    lines = text.split("\n")
    title = []
    for index, line in enumerate(lines):
        res = re.match(r"subject: ", line.lower())
        if res:
            title.append(re.split(r"subject: ", line.lower())[-1].strip())
            start_index = index
            break
    for i in range(start_index+1, start_index+10):
        if 'summary of change' not in lines[i].lower():
            title.append(lines[i])
        else:
            break
    return (" ".join(title).strip().capitalize())

function_dict = {
    'date_from_text':get_date_from_text,
    'title_from_text':get_title_from_text
    }

def extract_attributes(df, bucket, api):
    print(bucket, api)
    if (bucket != '') and (api != ''):
        sub_type = str(df['extrctn_client_name'][0]).lower()
        if sub_type == 'transmittals_and_mln_articles':
            attribute_list = []
            for i, j in df.iterrows():
                temp = {
                        'date_from_text':"",
                        'title_from_text':""
                    }
                if j['dwnld_status_code'] == 200:
                    try:
                        uri = j['dwnld_s3_uri'].replace(f's3://{bucket}/','')
                        print(uri)
                        pdf_byte,text = read_pdf_from_s3(bucket, uri)
                        if len(text.split()) > 50:
                            for attributes in temp.keys():
                                temp[attributes] = function_dict[attributes](j['pdf_links'], text)  
                        else:
                            print(f"{uri} is a scanned PDF") 
                            continue  
                    except Exception as e:
                        print("Error:- ", e)
                        exception_type, exception_value, exception_traceback = sys.exc_info()
                        line_number = traceback.extract_tb(exception_traceback)[-1][1]
                        print(f"Error::- {e} \n Line Number :- {line_number} \n Exception Type :- {exception_type}")
                    temp['pdf_links'] = j['pdf_links']
                    attribute_list.append(temp)
        else:
            attribute_list = []
        return attribute_list, bucket, api
    
def save_file_to_s3(df, att_lst, batch_0,batch_1, bucket, api, env_secret, client, out_filename=None):
    print(bucket, api, env_secret, client)
    if (bucket != '') and (api != ''):
        sub_type = str(df['extrctn_client_name'][0]).lower()
        auth_tok = get_auth_token(env_secret)
        if sub_type == 'transmittals_and_mln_articles':
            attr_df = pd.DataFrame(att_lst)
            process_df = process_files_trans_art(df)
            final_df = pd.merge(process_df, attr_df, how='left', left_on=['pdf_links'], right_on=['pdf_links'])
            final_df['keywords'] = final_df['CR #'] + ', ' + final_df['policy_number'] + ', ' + final_df['title'] 
            final_df['date_from_webpage'] = pd.to_datetime(final_df['date_from_webpage'], errors='coerce').dt.strftime('%Y-%m-%d')
            final_df['date_from_text'] = pd.to_datetime(final_df['date_from_text'], errors='coerce').dt.strftime('%Y-%m-%d') 
            final_df['effective_date']=final_df.apply(lambda row: row['date_from_webpage'] if pd.isnull(row['date_from_text']) else row['date_from_text'], axis=1) 
            final_df.loc[final_df['policy_type']=='transmittals', 'title'] = final_df[final_df['policy_type']=='transmittals'].apply(lambda row: row['title_from_text'] if pd.isnull(row['title']) else row['title'], axis=1)        
        elif sub_type == 'publications':
            final_df = process_files_pub(df)
            final_df['keywords'] = final_df['Format'] + ', ' + final_df['policy_number'] + ', ' + final_df['title'] + ', ' + final_df['Topic'] 
            final_df['effective_date'] = pd.to_datetime(final_df['effective_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # common operations
        final_df['payor'] = APPLICATION_DEST_FOLDER_LEVEL_1.upper()
        final_df = final_df.drop(['extrctn_client_name'], axis=1)
        cols = add_column_prefix(final_df, COLUMN_PREFIX)
        final_df.columns = cols      
        final_df['abstrctn_policy_number'] = final_df['abstrctn_policy_number'].apply(lambda x: x.upper())
        raw_csv_buffer = io.BytesIO()
        final_df.to_csv(raw_csv_buffer, index=False)
        raw_csv_buffer.seek(0)
        raw_out_filename = f"{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/{RAW_DEST_FOLDER_LEVEL_7}/{batch_0}_{batch_1}_{sub_type}_{RAW_DEST_FOLDER_LEVEL_5}_policies_abstraction.csv"
        s3_client.upload_fileobj(raw_csv_buffer, bucket, raw_out_filename)
        print(f"Raw extract uploaded to S3 bucket {bucket} successfully")

        final_df.rename(columns={'abstrctn_title':'document_name', 'abstrctn_line_of_business':'line_of_business_id',  'abstrctn_service_level':'service_level_id', 'abstrctn_state':'state_id',
                                'abstrctn_payor':'payor_id','abstrctn_policy_type':'document_type_id', 'abstrctn_effective_date':'effective_date',
                                'abstrctn_keywords':'keywords', 'abstrctn_policy_number':'payor_policy_number','abstrctn_pdf_links':'original_document_link', 
                                'abstrctn_dwnld_file_name':'zignaai_document_url'}, inplace=True)
        
        
        print("dataframe shape before dropping null document name policies", final_df.shape)
        final_df.dropna(subset=['document_name'], inplace=True)
        print('dataframe shape after dropping null document name policies', final_df.shape)
        pattern = re.compile('[^a-zA-Z0-9 ]+')
        final_df['document_name'] = final_df['document_name'].apply(lambda x: re.sub(pattern, '_', x))
        final_df['payor_policy_number'] = final_df['payor_policy_number'].fillna('null')
        final_df['affiliate_payor_id'] = 'null'
        final_df['uuid']=''
        pattern = re.compile('[^a-zA-Z0-9-_ %]+')
        for i, j in final_df.iterrows():
            if j['payor_policy_number']=='null':
                final_df.at[i, 'uuid'] = j['payor_id']+'_'+j['affiliate_payor_id']+'_'+ (re.sub(pattern,'', j['original_document_link'].split('/')[-1].split('.')[0])).lower().replace('%20','_') .replace('-', '_').replace(' ','_')
            else:
                final_df.at[i, 'uuid'] = j['payor_id']+'_'+j['affiliate_payor_id']+'_'+ j['payor_policy_number']
        final_df['uuid'] = final_df['uuid'].apply(lambda x: x.lower().replace('_null_', '__'))
        for col in ['line_of_business_id','document_type_id', 'payor_id']:
            final_df[col].fillna("null", inplace=True)
        
        for tuple_value in [('line_of_business_id', lineofbusiness_map), ('document_type_id', document_type_map)]:
            key_id_dict_lower = {k.lower(): v for k, v in tuple_value[1].items()}
            final_df[tuple_value[0]] = (final_df[tuple_value[0]].str.lower().replace(key_id_dict_lower)).astype(str)

        mapping_list = [(final_df,'line_of_business_id',all_ids_per_category(api, 'getAllBusinessLines', auth_tok)), 
                    (final_df,'document_type_id',all_ids_per_category(api, 'getAllPoliciesTypes', auth_tok)), 
                    (final_df,'payor_id',payor_id_mapping_fn(api,auth_tok))]
        for mapper in mapping_list:
            final_df = replace_keys_with_ids(mapper[0],mapper[1],mapper[2])
        print('id mapping successful')
        final_df_grouped= final_df.groupby(['uuid']).agg({
            'document_name':'first',
            'original_document_link': 'first', 
            'affiliate_payor_id':'first', 
            'zignaai_document_url': 'first', 
            'line_of_business_id': 'first',
            'service_level_id': 'first',
            'state_id': 'first',
            'payor_id': 'first',
            'document_type_id': 'first',
            'effective_date': 'first',
            'keywords': 'first',
            'payor_policy_number': 'first'
        }).reset_index()
        final_df_grouped['service_level_id'] = final_df_grouped['service_level_id'].apply(lambda x: 'null' if x not in ('1', '2') else x)
        final_df['retired'] = 'N'
        final_df['end_date'] = ''
        final_df['contractor_id'] = ''
        final_df['contractor_type_id'] = ''
        final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
        final_df['document_name'] = final_df['payor_policy_number'] + " -" + final_df['document_name']

        final_df = final_df[['uuid', 'document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id', 'affiliate_payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contractor_type_id', 'retired']]
        
        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False,sep='|')
        csv_buffer.seek(0)
        current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        application_file_name = current_time + "_" + sub_type.lower() +  "_" + str(batch_0) + "_" + str(batch_1) + ".txt"
        application_out_filepath = f"{client}/{APPLICATION_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_3}/{application_file_name}"

        job_type = "policy catalog"
        comment = 'abstraction file inserted'

        s3_client.upload_fileobj(csv_buffer, bucket, application_out_filepath)
        print(f"Application Files uploaded to S3 bucket {bucket} successfully")
        # api_url = f'{api}insert_task_into_queue'
        # status_code = send_payload(api_url, application_file_name, job_type,comment, auth_tok)
        # print(f"API hit: {status_code}")
    return "Files uploaded to S3 bucket"
