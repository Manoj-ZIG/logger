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
from constants import get_auth_token, replace_keys_with_ids, payor_id_mapping_fn,all_ids_per_category, add_column_prefix, document_type_map, affiliate_map, states_map, lineofbusiness_map, service_level_map, contract_type_map, contractor_name_map
from constants import APPLICATION_DEST_FOLDER_LEVEL_2, APPLICATION_DEST_FOLDER_LEVEL_3, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2, RAW_DEST_FOLDER_LEVEL_4, RAW_DEST_FOLDER_LEVEL_5, RAW_DEST_FOLDER_LEVEL_6, RAW_DEST_FOLDER_LEVEL_7, DOWNLOAD_FOLDER
from uhc_attributes_extraction import *
import traceback
import sys
from constants import COLUMN_PREFIX,s3_client, ssm
from decimal import Decimal
import requests
import uuid

# custom import
from send_payload import send_payload

APPLICATION_DEST_FOLDER_LEVEL_1 = 'uhc' 
client_name = 'uhc'
policies_to_run_in_ec2 = []

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

def read_attachment_from_s3(bucket, object_key):    
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=object_key)
        pdf_byte = resp['Body'].read()
        text = extract_text(BytesIO(pdf_byte), page_numbers=range(100))
        return text
    except Exception as e:
        print("Error: File not found", e)
        return ""

def get_policy_type(pdflink, text):
    policy_type = 'NA'
    policy_type = policyType(pdflink, text)
    print('Executed policy type extraction')
    return policy_type

def get_policy_number(text):
    try:
        policy_number = 'NA'
        policy_number_types = [('Policy Number','2023F7013E'),('Policy Number:','DIAGNOSTIC 105.25'),('Policy Number:','CS002NJ.AA'),
        ('Policy Number:','BIP001.L'),('Policy Number:','SURGERY 030.32'),('Policy Number:','DCP048.02'), ('Guideline Number:','CS356LA.A')]

        for pattern in policy_number_types:
            policy_number = extract_policy_number(pattern[0], pattern[1], text)
            policy_number = re.sub(r'[^A-Za-z0-9.]+', '-', policy_number)
            if policy_number:
                break
    except Exception as e:
        print("Error: ", e)
        policy_number = 'NA'
    print('Executed policy number extraction')
    return policy_number


def get_effective_date(text):
    try:
        effective_date = 'NA'
        effective_date_patterns = ['History','Policy History/Revision Information','Guidline History/Revision Information', 'Policy History']
        effective_date = check_effective_date(text)
        if effective_date == 'NA':
            for pattern in effective_date_patterns:
                effective_date = check_effective_date_history(text, pattern)
                if effective_date != 'NA':
                    break
        if effective_date == 'NA':
            effective_date = nt_check_effective_date(text)
    except Exception as e:
        print("Error :- ", e)
        effective_date = 'NA'
    print('Executed effective date extraction')
    return effective_date

def get_related_attachments(bucket, object_key):
    try:
        attachments = extract_hyperlinks(bucket, object_key)
        if not attachments:
            attachments = ["NA"]
        else:
            attachments = list(set(attachments))
    except Exception as e:
        print("Error :- ", e)
        attachments = ['NA']
    print('Executed related attachments extraction')
    return attachments

def get_state_exception(text):
    states = ['NA']
    df = section_cleanup(text)
    df3 = section_range(df)
    lines = text.split("\n")
    state_sections = ['Application', 'State Exceptions', 'Policy', 'Applicable States', 'Exceptions']
    for section_name in state_sections:
        if section_name in df3['section'].to_list():
            section_text = extract_text_for_section(df3, section_name, lines)
            section_text_para = " ".join(section_text)
            states = state_exceptions(section_text)
            if not states:
                continue
            else:
                if section_name not in ['Exceptions', 'State Exceptions'] and any(phrase in section_text_para.lower() for phrase in ["does not apply", "except for", "states exempt from policy", "excluded from", "exempt from"]):
                    states = list(set(states_list) - set(states))
                elif section_name in ['Exceptions', 'State Exceptions'] or "plan exceptions" in section_text_para.lower():
                    states = states_list
                break
    return states

def get_states(pdf_link, text):
    state = ['NA']
    try:
        if len(pdf_link.split("/")[-1].split(".")[0].split("-")) >=2:
            st = [pdf_link.split("/")[-1].split(".")[0].split("-")[-1].upper(), pdf_link.split("/")[-1].split(".")[0].split("-")[-2].upper(), pdf_link.split("/")[-2].upper()]
            for st_element in st:
                if ((len(str(st_element)) == 2) and (st_element in state_abbreviation_list)):
                    state = [k for k, v in state_abbrev_mapping.items() if v == st_element]
                    if state != ['NA']:
                        break
                    else:
                        continue
        if state == ['NA']:
            initial_text = text.split("\n")[:15]
            state = state_exceptions(initial_text)
            if not state:
                state = ['NA']
        if state == ['NA']:
            state = get_state_exception(text)
        print('Executed states extraction')
        return state
    except Exception as e:
        state = ['NA']
        print("State Error: ", e)
        print('Executed states extraction')
        return state
    
def get_states_new_pdf(text):
    state = ['NA']
    try:
        initial_text = text.split("\n")[:15]
        state = state_exceptions(initial_text)
        if not state:
            state = ['NA']
        if state == ['NA']:
            state = get_state_exception(text)
        print('Executed states extraction')
        return state
    except Exception as e:
        state = ['NA']
        print("State Error: ", e)
        print('Executed states extraction')
        return state

def get_service_level(text):
    try:
        lines = text.split("\n")
        sl_values = []
        for line in lines:
            final_line = (re.sub(r'[^a-zA-Z0-9\s]+', '', line)).lower()
            final_line = re.sub(' +', ' ', final_line)
            for word in ['professional', 'facility', 'cms1500', 'cms 1500', 'cms1450', 'cms 1450', 'ub04', 'ub 04']:
                if word in final_line:
                    sl_values.append(word)
        sl_values_list = list(set(list(map(lambda x: "ub04" if x == 'ub 04' else ("cms1500" if x == 'cms 1500' else ("cms1450" if x == 'cms 1450' else x)), sl_values))))
        sorted_tuple_sl_values = tuple(sorted(sl_values_list))
        if sorted_tuple_sl_values in uhc_service_level_dict.keys():
            final_service_level = uhc_service_level_dict[sorted_tuple_sl_values]
        else:
            final_service_level = 'All'
    except Exception as e:
        final_service_level = 'All'
        print("State Error: ", e)
    print('Executed service level extraction')
    return final_service_level

def bulletins_processing(df):
    for index, row in df.iterrows():
        try:
            if (row['abstrctn_policy_type'] == "Bulletins & Updates"):
                month = row['abstrctn_pdf_links'].split("/")[-1].split(".")[0].split("-")[-2]
                year = row['abstrctn_pdf_links'].split("/")[-1].split(".")[0].split("-")[-1]
                day = '01'
                date = day + " " + month + " "+  year
                date_formats = {r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})" : '%d %B %Y',
                                r"\d{1,2}\s+(Jan|Feb|Mar|Apr|May|June|Jul|Aug|Sept|Oct|Nov|Dec)\s+\d{4}" : '%d %b %Y'}
                for pattern, date_format in date_formats.items():
                    if re.match(pattern, date, re.IGNORECASE):
                        eff_date = datetime.strptime(date, date_format).strftime('%Y-%m-%d')
                        df.at[index, 'abstrctn_effective_date'] = eff_date
        except Exception as e:
            continue
    
    mask = (df['abstrctn_policy_type'] == "Bulletins & Updates") & (df['abstrctn_policy_title'].str.contains("Archive"))
    df = df[~mask]
    print('Executed bulletins processing')
    return df

def code_ranges_extraction(lines):
    l1 = []
    for line in lines:
        for pattern in code_range_patterns_list:
            matches = re.findall(pattern, line)
            if matches:
                l1.extend(code_range(matches))
    return l1

def get_list_of_codes(text_lines):
    codes_list = []
    code_range_list = []
    final_codes = []
    try:
        code_range_list = code_ranges_extraction(text_lines) 
        codes_list = codes_extraction_with_type(text_lines)
        final_codes = code_range_list + codes_list
        final_codes = list(set(final_codes))
    except Exception as e:
        print("Error : - ", e)
    print('Executed code extraction')
    return final_codes

def get_codes_from_attachment(bucket, uri):
    codes_from_attachment = []
    try:
        attachments = get_related_attachments(bucket, uri)
        if attachments == ['NA']:
            return codes_from_attachment, policies_to_run_in_ec2
        code_attach_list = [attachment for attachment in attachments if ('attachments' in attachment) and attachment.endswith(".pdf")]
        if code_attach_list == []:
            return codes_from_attachment, policies_to_run_in_ec2
        for attachment_pdf in code_attach_list[:5]:
            response = requests.get(attachment_pdf)
            if response.status_code == 200:
                fname = "_".join(attachment_pdf.split('/')[-3:]).replace('.pdf','').replace('%20','_')
                pattern = re.compile('[^a-zA-Z0-9]+')
                file_name = re.sub(pattern, '_', fname) + '.pdf'
                s3_path = f'{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{DOWNLOAD_FOLDER}/code_attachments/{file_name}'
                s3_client.put_object(Bucket=bucket, Key=s3_path, Body=response.content, ContentType='application/pdf')
                print("Download of code attachment completed")
                s3_object = s3_client.get_object(Bucket=bucket, Key=s3_path)
                file_stream = BytesIO(s3_object['Body'].read())
                with pikepdf.open(file_stream) as pdf:
                    num_pages = int(len(pdf.pages))
                    print(f"Number of pages in {file_name} attachment: {num_pages}")
                    if num_pages <= 100:
                        pdf_byte,text = read_pdf_from_s3(bucket, s3_path)
                        if len(text.split()) > 50:
                            section_name = 'References'
                            lines = text.split("\n")
                            section_text_lines = lines
                            df = section_cleanup(text)
                            df3 = section_range(df)
                            if len(df3) != 0:
                                if section_name in df3['section'].to_list():
                                    print('Reference section found in the Attachment')
                                    section_text_lines = remove_section_text(df3, section_name, lines)
                            codes_from_attachment.extend(get_list_of_codes(section_text_lines))
                            if codes_from_attachment:
                                print('Executed code extraction from attachment')
                    else:
                        dwnld_url = f's3://{bucket}/{uri}'
                        policies_to_run_in_ec2.append(dwnld_url)
                        text = read_attachment_from_s3(bucket, s3_path)
                        if len(text.split()) > 50:
                            section_name = 'References'
                            lines = text.split("\n")
                            section_text_lines = lines
                            df = section_cleanup(text)
                            df3 = section_range(df)
                            if len(df3) != 0:
                                if section_name in df3['section'].to_list():
                                    print('Reference section found in the Attachment')
                                    section_text_lines = remove_section_text(df3, section_name, lines)
                            codes_from_attachment.extend(get_list_of_codes(section_text_lines))
                            if codes_from_attachment:
                                print('Executed code extraction from attachment')
        return codes_from_attachment, policies_to_run_in_ec2
    except Exception as e:
        print("Error : - ", e)
        return codes_from_attachment, policies_to_run_in_ec2

def get_all_codes(text, bucket, uri):
    codes_from_pdf = []
    codes_from_attachment = []
    all_codes = []
    section_name = 'References'
    lines = text.split("\n")
    try:
        section_text_lines = lines
        df = section_cleanup(text)
        df3 = section_range(df)
        if len(df3) != 0:
            if section_name in df3['section'].to_list():
                print('Reference section found in the PDF')
                section_text_lines = remove_section_text(df3, section_name, lines)                
        codes_from_pdf = get_list_of_codes(section_text_lines) 
        codes_from_attachment = get_codes_from_attachment(bucket, uri)[0]
        all_codes = codes_from_pdf + codes_from_attachment
        all_codes = list(set(all_codes))
    except Exception as e:
        print("Error : - ", e)
    print('Executed code extraction')
    return all_codes

def get_line_of_business_from_content_pdflink(pdf_link, text):
    initial_para = ("".join(text.split('\n')[:10])).lower().replace("  ", " ")
    final_lob = 'NA'
    found_lob = False
    for lob in line_of_business_dict.keys():
        print(lob)
        if found_lob:
            break 

        if (lob in [link_element.lower() for link_element in pdf_link.split('/')]) or (lob in ((pdf_link.split("/")[-1]).lower()).replace("_","-")):
            lob_value = line_of_business_dict[lob] 
            if isinstance(lob_value, dict):
                print("is a dictionary")
                for lob_key in lob_value:
                    if lob_key in initial_para:
                        final_lob = lob_value[lob_key]
                        print(final_lob)
                        if final_lob != 'NA':
                            found_lob = True
                            break
            else:
                print("not a dictioanary")
                final_lob = lob_value
    print('Executed line of business extraction')
    return final_lob

function_dict = {
    'policy_type': get_policy_type,
    'policy_number': get_policy_number,
    'effective_date': get_effective_date,
    'list_of_codes': get_all_codes,
    'state_inclusions': get_states,
    'related_attachments': get_related_attachments,
    'service_level':get_service_level,
    'pdf_content_lob': get_line_of_business_from_content_pdflink,
    'pdf_content_states_new': get_states_new_pdf
    }

def extract_attributes(df, bucket, api, env_secret):
    print(bucket, api, env_secret)
    if (bucket != '') and (api != ''):
        auth_tok = get_auth_token(env_secret)
        attribute_list = []
        print(f'Length of input dataframe {len(df)}')
        print(f"No. of unique pdf links in df {df['extrctn_pdf_links'].nunique()}")
        dedup_df = df.drop_duplicates(subset=['extrctn_pdf_links'], keep='first')
        print(f'Length of deduped dataframe {len(dedup_df)}')
        print(f"No. of unique pdf links in deduped df {dedup_df['extrctn_pdf_links'].nunique()}")
        for i, j in dedup_df.iterrows():
            temp = {
                    'policy_type': "",
                    'policy_number': "",
                    'effective_date': "",
                    'list_of_codes': "",
                    'state_inclusions': "",
                    'related_attachments': "",
                    'service_level': "",
                    'pdf_content_lob':"",
                    'pdf_content_states_new':""
                }
            if j['dwnld_status_code'] == 200:
                try:
                    uri = j['dwnld_s3_uri'].replace(f's3://{bucket}/','')
                    print(uri)
                    pdf_byte,text = read_pdf_from_s3(bucket, uri)
                    if len(text.split()) > 50:
                        for attributes in temp.keys():
                            if not attributes in ['state_inclusions', 'related_attachments', 'policy_type', 'list_of_codes', 'pdf_content_lob']:
                                temp[attributes] = function_dict[attributes](text)
                            elif attributes in ['list_of_codes']:
                                temp[attributes] = function_dict[attributes](text, bucket, uri)
                            elif attributes in ['related_attachments']:
                                temp[attributes] = function_dict[attributes](bucket, uri)
                            elif attributes in ['state_inclusions', 'policy_type', 'pdf_content_lob']:
                                temp[attributes] = function_dict[attributes](j['extrctn_pdf_links'], text)  
                    else:
                        print(f"{uri} is a scanned PDF") 
                        continue  
                except Exception as e:
                    print("Error:- ", e)
                    exception_type, exception_value, exception_traceback = sys.exc_info()
                    line_number = traceback.extract_tb(exception_traceback)[-1][1]
                    print(f"Error::- {e} \n Line Number :- {line_number} \n Exception Type :- {exception_type}")
                temp['object_key'] = j['dwnld_s3_uri']
                filename_pattern = re.compile('[^a-zA-Z0-9.]+')
                temp['pdf_name'] = re.sub(filename_pattern, '_', j['dwnld_file_name'].split('.')[0])
                temp['policy_title'] = (' '.join((j['extrctn_pdf_links'].split("/")[-1].split('.pdf')[0].lower()).split('-'))).title()
                for word in remove_word_title_list:
                    if word in temp['policy_title']:
                        temp['policy_title'] = temp['policy_title'].replace(word, "").strip()
                temp['pdf_links'] = j['extrctn_pdf_links']
                attribute_list.append(temp)
        return attribute_list, bucket, api, auth_tok

def save_file_to_s3(df,attributes_list, batch_0,batch_1, bucket, api, token, client, out_filename=None):
    '''
    This function saves the extracted attributes to a csv file in S3 bucket
    args:
        attributes_list: list of dictionaries containing extracted attributes
        batch: batch number
        out_filename: name of the file to be saved in S3 bucket
    return:
        None
    '''
    print(bucket, api, token, client)
    if (bucket != '') and (bucket != ""):
        print(f"Batch: {batch_0}_{batch_1} is currently executing post generating attribute list")
        df1 = pd.DataFrame(attributes_list)
        df1.columns = add_column_prefix(df1, COLUMN_PREFIX)

        merged_df = pd.merge(df, df1, left_on = 'extrctn_pdf_links', right_on = 'abstrctn_pdf_links', how = 'left')
        print("shape of merged df before droppping scanned PDFs is: ", merged_df.shape)
        merged_df = merged_df[~(merged_df['abstrctn_pdf_name'].isna())]
        print("shape of merged df after droppping scanned PDFs is: ", merged_df.shape)

        print("unique pdf links in merged df: ", merged_df['extrctn_pdf_links'].nunique())
        merged_df['abstrctn_policy_number'] = merged_df['abstrctn_policy_number'].astype(str)
        merged_df['abstrctn_state_inclusions'] = merged_df['abstrctn_state_inclusions'].fillna("")
        merged_df['extrctn_state'] = merged_df['extrctn_state'].apply(lambda x: eval(x))
        merged_df['abstrctn_states'] = ""

        for i, j in merged_df.iterrows():
            if (j['abstrctn_state_inclusions'] != []) and (j['abstrctn_state_inclusions'] != ['NA']) and (j['abstrctn_state_inclusions'] != ""):
                merged_df.at[i, 'abstrctn_states'] = list(set(j['extrctn_state']).intersection(set(j['abstrctn_state_inclusions'])))
            else:
                merged_df.at[i, 'abstrctn_states'] = j['extrctn_state']

        for i, j in merged_df.iterrows():
            if j['extrctn_site_name'] == "https://www.geha.com":
                merged_df.at[i, 'abstrctn_effective_date'] = j['extrctn_effective_date']

        for index, row in merged_df.iterrows():
            if row['abstrctn_policy_type'] == 'Clinical Guideline':
                merged_df.at[index, 'abstrctn_line_of_business'] = 'NA'
                # merged_df.at[index, 'abstrctn_effective_date'] = 'NA'
                # merged_df.at[index, 'abstrctn_policy_number'] = 'NA'
                # merged_df.at[index, 'abstrctn_state_inclusions'] = 'NA' 
                # merged_df.at[index, 'abstrctn_list_of_codes'] = 'NA'  

        final_df = bulletins_processing(merged_df)
        final_df['ec2_run'] = final_df['dwnld_s3_uri'].isin(policies_to_run_in_ec2).astype(int)
        print("policies to run in ec2 are: ", policies_to_run_in_ec2)
        raw_csv_buffer = io.BytesIO()
        final_df.to_csv(raw_csv_buffer, index=False)
        raw_csv_buffer.seek(0)
        raw_out_filename = f"{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/{RAW_DEST_FOLDER_LEVEL_7}/{batch_0}_{batch_1}_{APPLICATION_DEST_FOLDER_LEVEL_1}_{RAW_DEST_FOLDER_LEVEL_5}_policies_abstraction.csv"
        s3_client.upload_fileobj(raw_csv_buffer, bucket, raw_out_filename)
        print(f"Raw extract uploaded to S3 bucket {bucket} successfully")

        final_df.rename(columns={'abstrctn_policy_title':'document_name', 'abstrctn_effective_date':'effective_date', 'abstrctn_list_of_codes':'keywords', 'abstrctn_service_level':'service_level_id',
                                'extrctn_client_name':'payor_id', 'extrctn_affiliate': 'affiliate_payor_id', 'abstrctn_pdf_links':'original_document_link', 'abstrctn_states':'state_id',
                                'extrctn_line_of_business':'line_of_business_id', 'abstrctn_policy_type':'document_type_id', 'abstrctn_policy_number':'payor_policy_number'}, inplace=True)

        # final_df = final_df[final_df['ec2_run'] == 0]
        final_df['effective_date'] = pd.to_datetime(final_df['effective_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
        final_df['zignaai_document_url'] = final_df['abstrctn_pdf_name'].apply(lambda x : x + '.pdf')

        final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id',
                        'affiliate_payor_id', 'document_type_id', 'effective_date', 'last_refresh_date', 'keywords', 'payor_policy_number']]
        
        final_df['keywords'] = final_df['keywords'].apply(lambda lst: ', '.join(lst))
        final_df['keywords'] = final_df['keywords'] + ', ' + final_df['payor_policy_number']
        
        print("shape of final df before explode is: ", final_df.shape)
        final_df['line_of_business_id'] = final_df['line_of_business_id'].apply(lambda x: eval(x) if isinstance(x, str) and x.startswith("[") else x)
        final_df = final_df.explode('state_id')
        print("shape of final df after explode state is: ", final_df.shape)
        final_df = final_df.explode('service_level_id')
        print("shape of final df after explode service level is: ", final_df.shape)
        final_df = final_df.explode('line_of_business_id')
        print("shape of final df after explode lob is: ", final_df.shape)

        print(final_df['line_of_business_id'].isna().sum())
        print(final_df['service_level_id'].isna().sum())
        print(final_df['state_id'].isna().sum())
        print(final_df['document_type_id'].isna().sum())
        print(final_df['payor_id'].isna().sum())
        print(final_df['affiliate_payor_id'].isna().sum())

        final_df['payor_policy_number'] = final_df['payor_policy_number'].fillna('null')
        final_df['payor_policy_number'] = final_df['payor_policy_number'].replace('NA', 'null')

        # final_df['doc_id_by_policy_number']=''
        # pattern = re.compile('[^a-zA-Z0-9-_ %]+')
        # for i, j in final_df.iterrows():
        #     if j['payor_policy_number']=='null':
        #         final_df.at[i, 'doc_id_by_policy_number'] = j['payor_id']+'_'+j['affiliate_payor_id']+'_'+ (re.sub(pattern,'', j['original_document_link'].split('/')[-1].split('.')[0])).lower().replace('%20',' ') .replace('-', ' ').replace('_',' ')
        #     else:
        #         final_df.at[i, 'doc_id_by_policy_number'] = j['payor_id']+'_'+j['affiliate_payor_id']+'_'+ j['payor_policy_number']
        # final_df['doc_id_by_policy_number'] = final_df['doc_id_by_policy_number'].apply(lambda x: x.lower().replace('_null_', '__'))

        # final_df['doc_id__pn_and_name']=''
        # for i, j in final_df.iterrows():
        #     final_df.at[i, 'doc_id__pn_and_name'] = j['payor_id']+'_'+j['affiliate_payor_id']+'_'+ j['payor_policy_number']+'_'+ (re.sub(pattern,'', j['original_document_link'].split('/')[-1].split('.')[0])).lower().replace('%20',' ') .replace('-', ' ').replace('_',' ')
        # final_df['doc_id__pn_and_name'] = final_df['doc_id__pn_and_name'].apply(lambda x: x.lower().replace('_null_', '__'))

        # final_df['uuid'] = [uuid.uuid4() for _ in range(len(final_df.index))]


        for col in ['line_of_business_id', 'service_level_id', 'state_id', 'document_type_id', 'payor_id', 'affiliate_payor_id']:
            final_df[col].fillna("null", inplace=True)
        
        for tuple_value in [('line_of_business_id', lineofbusiness_map), ('service_level_id', service_level_map), ('state_id', states_map), ('document_type_id', document_type_map), ('affiliate_payor_id', affiliate_map)]:
            key_id_dict_lower = {k.lower(): v for k, v in tuple_value[1].items()}
            final_df[tuple_value[0]] = (final_df[tuple_value[0]].str.lower().replace(key_id_dict_lower)).astype(str)

        mapping_list = [(final_df,'line_of_business_id',all_ids_per_category(api, 'getAllBusinessLines', token)),
                    (final_df,'service_level_id',all_ids_per_category(api, 'getAllServiceLevels', token)),
                    (final_df,'state_id',all_ids_per_category(api, 'getAllStates', token)),
                    (final_df,'document_type_id',all_ids_per_category(api, 'getAllPoliciesTypes', token)),
                    (final_df,'payor_id',payor_id_mapping_fn(api, token)),
                    (final_df,'affiliate_payor_id',all_ids_per_category(api, 'getAllAffiliates', token))]
        for mapper in mapping_list:
            final_df = replace_keys_with_ids(mapper[0],mapper[1],mapper[2])
            time.sleep(3)
        print('id mapping successful')
        final_df= final_df.groupby(['document_name', 'affiliate_payor_id']).agg({
            'original_document_link': 'first', 
            'zignaai_document_url': 'first', 
            'line_of_business_id': lambda x: ','.join(set(x)),
            'service_level_id': lambda x: ','.join(set(x)),
            'state_id': lambda x: ','.join(set(x)),
            'payor_id': 'first',
            'document_type_id': 'first',
            'effective_date': 'first', 
            'last_refresh_date': 'first',
            'keywords': 'first',
            'payor_policy_number': 'first'
            
        }).reset_index()
        final_df['service_level_id'] = final_df['service_level_id'].apply(lambda x: 'null' if x not in ('1', '2') else x)
        
        final_df['end_date'] = ''
        final_df['contractor_id'] = ''
        final_df['contractor_type_id'] = ''
        final_df['retired'] = 'N'

        final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id',
                             'payor_id', 'affiliate_payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contractor_type_id', 'retired']]
        print("shape of final df after grouping is: ", final_df.shape)

        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, sep = '|', index=False)
        csv_buffer.seek(0)
        current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        application_file_name = current_time + "_" + APPLICATION_DEST_FOLDER_LEVEL_1.lower() + "_" + str(batch_0) + "_" + str(batch_1) + ".txt"
        application_out_filepath = f"{client}/{APPLICATION_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_3}/{application_file_name}"
        job_type = "policy catalog"
        comment = 'abstraction file inserted'

        s3_client.upload_fileobj(csv_buffer, bucket, application_out_filepath)
        print("Application Files uploaded to S3 bucket successfully")
        api_url = f'{api}insert_task_into_queue'
        status_code = send_payload(api_url, application_out_filepath, job_type,comment, token)
        print(f"API hit: {status_code}")
    return "Application Files uploaded to S3 bucket successfully"