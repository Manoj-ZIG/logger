import re
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
from constants import get_auth_token, replace_keys_with_ids, payor_id_mapping_fn, add_column_prefix
from constants import APPLICATION_DEST_FOLDER_LEVEL_2, APPLICATION_DEST_FOLDER_LEVEL_3, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2, RAW_DEST_FOLDER_LEVEL_4, RAW_DEST_FOLDER_LEVEL_5, RAW_DEST_FOLDER_LEVEL_6, RAW_DEST_FOLDER_LEVEL_7, DOWNLOAD_FOLDER
from uhc_attributes_extraction import *
import traceback
import sys
from constants import COLUMN_PREFIX,s3_client, ssm
from decimal import Decimal
import requests

# custom import
from send_payload import send_payload

APPLICATION_DEST_FOLDER_LEVEL_1 = 'centene' 
client_name = 'centene'

#################### CONSTANTS #####################
policy_type_list = ['Concert Genetic Testing:', 'Genetic testing:', 'Concert Oncology:', 'Drug Policy:', 'State Policy:',
                        'Payment Policy:', 'Concert Genetics Oncology:', 'Payment Integrity Policy:',
                        'Pharmacy Policy:', 'Clinical  Policy:','Transparency Policy:','Clinical Policy:',
                        'Behavioral Policy:', 'Vision Policy:', 'Research Clinical Care:', 'Practice Guidelines:', 
                        'Clinical practice guideline:', 'Claims and Payment Policy:',
                        'Claims Payment Policy:']

policy_types_titles = ['DEPARTMENT:', 'BUSINESS UNIT:']
policy_names_titles= ['DOCUMENT NAME:', 'POLICY NAME:']
last_review_date_matches = ['Last Review Date:', 'Date of Last Review:',
                         'Date of Approval by Committee:', 'Date of Last Revision:',
                         'Date ofLast revision:','APPROVED DATE:', 'Date Updated in Database:', 'Revised Date(s):']
effective_date_matches = ['Effective Date:', 'Original Effective Date:', 'Revised Effective Date(s):']
policy_number_list = ['Reference Number:','REFERENCE NUMBER:', 'POLICY ID:', 'P&P NUMBER:', 'Policy Number:']  
policy_number_pattern = '[A-Za-z]{1,4}\\-[A-Za-z|0-9]{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}(\\.|\\/)[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}\\.\\d{1,4}'
states_list = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
               "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
               "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
               "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
               "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
               "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
                'American Samoa','District of Columbia','Guam','North Mariana Islands','US Virgin Islands','Puerto Rico']
lob_dict_cen = {
    'commercial':'Commercial',
    'exchange': 'Marketplace',
    'medicare-advantage': 'Medicare Advantage',
    'medicare advantage':'Medicare Advantage',
    'comm-plan': 'Medicaid',
    'medicaid': 'Medicaid',
    'community': 'Medicaid'
}

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
    
def policyType_no_colon(text):
    policy = 'NA'
    d = {}
    for policy_type in policy_type_list:
        pt_wo = policy_type[:-1]
        result = re.findall(pt_wo.lower(), text.lower())
        d[pt_wo] = len(result)
        new_d = sorted(d.items(), key=lambda item: item[1], reverse=True)
        if new_d[0][1]>0:
            policy = new_d[0][0]
            break
    return policy

def get_policy_type_policy_name(text):
    
    pt = 'NA'
    pn = 'NA'
    line_text = text.split('\n')

    for line in line_text:
        for string in policy_type_list:
            if string.lower() in line.lower():
                pt = line.split(':')[0].strip()
                pn = line.split(':')[1].strip()         
                break
    if (pt == 'NA') and (pn == 'NA'):
        pt = (policyType_no_colon(text)).lower()
        if pt != 'na':
            for line in line_text:
                if pt.lower() in line.lower():
                    pn = line.lower().split(pt)[1].strip()
                    break 
        else:
            pt = 'NA'
            pn = 'NA'
    if pt =='NA':
            for i, line in enumerate(line_text):
                for policy_type in policy_types_titles:
                    if policy_type.lower() in line.lower():
                        pt = line.lower().split(':')[1].strip()
                        if not pt:
                            pt = line_text[i+1]
                            break
                for policy_name in policy_names_titles:
                    if policy_name.lower() in line.lower():
                        pn = line.lower().split(':')[1].strip()
                        if not pn:
                            pn = line_text[i+1]
                            break
    return pt, pn

def effective_date_regex(string, line):

    effective_date = re.findall(r"(?i){}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,2}}[-./, _:|]\d{{2,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?".format(string.lower())[:-1], line.lower()) or re.findall(r"(?i){}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}(?:st|nd|rd|th)?".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?\s+\d{{2,4}}\b".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?,\s+\d{{2,4}}\b".format(string.lower()[:-1]), line.lower()) or re.findall(
        r"(?i){}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{{1,2}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{4}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}})\b".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b\d{{1,4}},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i){}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}".format(string.lower()[:-1]), line.lower()) or re.findall(r"(?i)\b{}(?::)? (?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}},\s+\d{{2,4}}\b".format(string.lower()[:-1]), line.lower(), re.IGNORECASE)
    if effective_date:
        return effective_date[0]

def get_last_revision_dt_effective_dt(text):
    
    ldt = 'NA'
    edt = 'NA'
    line_text = text.split('\n')
    
    for i, line in enumerate(line_text):
        for string in last_review_date_matches:
            if string.lower() in line.lower():
                ldt = line.split(':')[1].strip()
                if not ldt:
                    ldt = line_text[i+1]
                    break
            if ldt == 'NA':
                ldt_output = effective_date_regex(string, line)
                if ldt_output:
                    ldt = ldt_output.lower().split(string.lower()[:-1])[-1].replace(':', '').strip()  
                    if not ldt:
                        ldt = line_text[i+1]
                        break
                      
    for i, line in enumerate(line_text):
        for match in effective_date_matches:
            if match.lower() in line.lower():
                edt = line.split(':')[1].strip()
                if not edt:
                    edt = line_text[i+1]
                    break
            if edt == 'NA':
                edt_output = effective_date_regex(match, line)
                if edt_output:
                    edt = edt_output.lower().split(match.lower()[:-1])[-1].replace(':', '').strip()  
                    if not edt:
                        edt = line_text[i+1]
                        break  
    return ldt, edt

def get_policy_number(text):
    pno = 'NA'
    line_text = text.split('\n')
    for line in line_text:     
        for string in policy_number_list:
            if string.lower() in line.lower():
                pno = line.split(':')[1].strip()
                pno = pno.upper()
                break
            if string.lower()[:-1] in line.lower():
                pno = line.lower().split(string.lower()[:-1])[1].strip()
                pno = pno.upper()
                break
    if pno == 'NA':
        pno = (re.search(policy_number_pattern, text).group()).upper()
    return pno

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

def get_all_codes(text):
    codes_from_pdf = []
    all_codes = []
    section_name = 'References'
    lines = text.split("\n")
    try:
        # section_text_lines = lines
        # df = section_cleanup(text)
        # df3 = section_range(df)
        # if len(df3) != 0:
        #     if section_name in df3['section'].to_list():
        #         print('Reference section found in the PDF')
        #         section_text_lines = remove_section_text(df3, section_name, lines)                
        codes_from_pdf = get_list_of_codes(lines) 
        all_codes = list(set(codes_from_pdf))
    except Exception as e:
        print("Error : - ", e)
    print('Executed code extraction')
    return all_codes

# service_level 

def get_lob_form_content(pdf_link, text):
    lob_value = ['NA']
    try:
        for lob in lob_dict_cen.keys():
            if lob in pdf_link.lower():
                lob_value = lob_dict_cen[lob]
                if lob_value != ['NA']:
                    break
                else:
                    continue
        if lob_value == ['NA']:
            lobs = []
            text_lines = text.split("\n")
            for lob in lob_dict_cen.keys():
                for line in text_lines:
                    if lob in line:
                        lobs.append(lob_dict_cen[lob])
                        lobs = list(set(lobs))
                    else:
                        continue
            lob_value = lobs
        return lob_value
    except Exception as e:
        lob_value = ['NA']
        print("LOB Error: ", e)
        return lob_value

def get_lob_from_content_labels(text):
    lob = 'NA'
    line_text = text.split('\n')
    strings_to_match = ['Line of Business', 'Product Type']
    for line in line_text:    
        for string in strings_to_match:
            if string.lower().strip() in line.lower():
                print(line.lower().split(string.lower().strip()))
                lob = line.lower().split(string.lower().strip())[1].replace(':',"").strip()
                break
    return lob

def state_exceptions(section_text):

    states = []
    for state in states_list:
        for line in section_text:
            if state in line:
                states.append(state)
                states = list(set(states))
            else:
                continue
    return states

def get_states_from_content(pdf_link, text):
    state = ['NA']
    try:
        for st in states_list:
            if st.lower() in pdf_link.lower().replace('-', ' '):
                state = st
                if state != ['NA']:
                    break
                else:
                    continue
        if state == ['NA']:
            # initial_text = text.split("\n")[:15]
            text_lines = text.split("\n")
            state = state_exceptions(text_lines)
        print('Executed states extraction')
        return state
    except Exception as e:
        state = ['NA']
        print("State Error: ", e)
        print('Executed states extraction')
        return state

function_dict = {
    'policy_type': lambda x: get_policy_type_policy_name(x)[0],
    'policy_title': lambda x: get_policy_type_policy_name(x)[1],
    'policy_number': get_policy_number,
    'effective_date': lambda x: get_last_revision_dt_effective_dt(x)[1],
    'last_review_date': lambda x: get_last_revision_dt_effective_dt(x)[0],
    'list_of_codes': get_all_codes,
    'states_from_content': get_states_from_content,
    'lob_from_content':get_lob_form_content,
    'lob_from_content_labels':get_lob_from_content_labels
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
                    'policy_title':"",
                    'policy_number': "",
                    'effective_date': "",
                    'last_review_date':"",
                    'list_of_codes': "",
                    'states_from_content':'',
                    'lob_from_content':'',
                    'lob_from_content_labels':''
                }
            if j['dwnld_status_code'] == 200:
                try:
                    uri = j['dwnld_s3_uri'].replace(f's3://{bucket}/','')
                    print(uri)
                    pdf_byte,text = read_pdf_from_s3(bucket, uri)
                    if len(text.split()) > 50:
                        for attributes in temp.keys():
                            if not attributes in ['states_from_content', 'lob_from_content']:
                                temp[attributes] = function_dict[attributes](text)
                            else:
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
                temp['pdf_name'] = re.sub(filename_pattern, '_', j['dwnld_file_name'].split('.pdf')[0])
                # temp['policy_title'] = (' '.join((j['extrctn_pdf_links'].split("/")[-1].split('.pdf')[0].lower()).split('-'))).title()
                # for word in remove_word_title_list:
                #     if word in temp['policy_title']:
                #         temp['policy_title'] = temp['policy_title'].replace(word, "").strip()
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

        # merged_df['abstrctn_policy_number'] = merged_df['abstrctn_policy_number'].astype(str)
        # merged_df['abstrctn_state_inclusions'] = merged_df['abstrctn_state_inclusions'].fillna("")
        # merged_df['extrctn_state'] = merged_df['extrctn_state'].apply(lambda x: eval(x))
        # merged_df['abstrctn_states'] = ""

        # for i, j in merged_df.iterrows():
        #     if (j['abstrctn_state_inclusions'] != []) and (j['abstrctn_state_inclusions'] != ['NA']) and (j['abstrctn_state_inclusions'] != ""):
        #         merged_df.at[i, 'abstrctn_states'] = list(set(j['extrctn_state']).intersection(set(j['abstrctn_state_inclusions'])))
        #     else:
        #         merged_df.at[i, 'abstrctn_states'] = j['extrctn_state']

        # for i, j in merged_df.iterrows():
        #     if j['extrctn_site_name'] == "https://www.geha.com":
        #         merged_df.at[i, 'abstrctn_effective_date'] = j['extrctn_effective_date']

        # for index, row in merged_df.iterrows():
        #     if row['abstrctn_policy_type'] == 'Clinical Guideline':
        #         merged_df.at[index, 'abstrctn_line_of_business'] = 'NA'
        #         # merged_df.at[index, 'abstrctn_effective_date'] = 'NA'
        #         # merged_df.at[index, 'abstrctn_policy_number'] = 'NA'
        #         # merged_df.at[index, 'abstrctn_state_inclusions'] = 'NA' 
        #         # merged_df.at[index, 'abstrctn_list_of_codes'] = 'NA'  

        # final_df = bulletins_processing(merged_df)
        # final_df['ec2_run'] = final_df['dwnld_s3_uri'].isin(policies_to_run_in_ec2).astype(int)
        # print("policies to run in ec2 are: ", policies_to_run_in_ec2)

        raw_csv_buffer = io.BytesIO()
        merged_df.to_csv(raw_csv_buffer, index=False)
        raw_csv_buffer.seek(0)
        raw_out_filename = f"{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/{RAW_DEST_FOLDER_LEVEL_7}/{batch_0}_{batch_1}_{APPLICATION_DEST_FOLDER_LEVEL_1}_{RAW_DEST_FOLDER_LEVEL_5}_policies_abstraction.csv"
        s3_client.upload_fileobj(raw_csv_buffer, bucket, raw_out_filename)
        print(f"Raw extract uploaded to S3 bucket {bucket} successfully")

        # final_df.rename(columns={'abstrctn_policy_title':'document_name', 'abstrctn_effective_date':'effective_date', 'abstrctn_list_of_codes':'keywords', 'abstrctn_service_level':'service_level_id',
        #                         'extrctn_client_name':'payor_id', 'extrctn_affiliate': 'affiliate_payor_id', 'abstrctn_pdf_links':'original_document_link', 'abstrctn_states':'state_id',
        #                         'extrctn_line_of_business':'line_of_business_id', 'abstrctn_policy_type':'document_type_id', 'abstrctn_policy_number':'payor_policy_number'}, inplace=True)

        # # final_df = final_df[final_df['ec2_run'] == 0]
        # final_df['effective_date'] = pd.to_datetime(final_df['effective_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
        # final_df['zignaai_document_url'] = final_df['abstrctn_pdf_name'].apply(lambda x : x + '.pdf')

        # final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id',
        #                 'affiliate_payor_id', 'document_type_id', 'effective_date', 'last_refresh_date', 'keywords', 'payor_policy_number']]
        
        # final_df['keywords'] = final_df['keywords'].apply(lambda lst: ', '.join(lst))
        # final_df['keywords'] = final_df['keywords'] + ', ' + final_df['payor_policy_number']
        # print("shape of final df before explode is: ", final_df.shape)

        # final_df = final_df.explode('state_id')
        # print("shape of final df after explode state is: ", final_df.shape)
        # final_df = final_df.explode('service_level_id')
        # print("shape of final df after explode service level is: ", final_df.shape)
        # final_df = final_df.explode('line_of_business_id')
        # print("shape of final df after explode lob is: ", final_df.shape)

        # print(final_df['line_of_business_id'].isna().sum())
        # print(final_df['service_level_id'].isna().sum())
        # print(final_df['state_id'].isna().sum())
        # print(final_df['document_type_id'].isna().sum())
        # print(final_df['payor_id'].isna().sum())
        # print(final_df['affiliate_payor_id'].isna().sum())

        # for col in ['line_of_business_id', 'service_level_id', 'state_id', 'document_type_id', 'payor_id', 'affiliate_payor_id']:
        #     final_df[col].fillna("null", inplace=True)

        # mapping_list = [(final_df,'line_of_business_id',line_of_business_id_mapping_fn(api, token)),
        #             (final_df,'service_level_id',service_level_id_mapping_fn(api, token)),
        #             (final_df,'state_id',state_id_mapping_fn(api, token)),
        #             (final_df,'document_type_id',document_type_id_mapping_fn(api, token)),
        #             (final_df,'payor_id',payor_id_mapping_fn(api, token)),
        #             (final_df,'affiliate_payor_id',affiliate_payor_id_mapping_fn(api, token))]
        # for mapper in mapping_list:
        #     final_df = replace_keys_with_ids(mapper[0],mapper[1],mapper[2])
        #     time.sleep(3)

        # final_df= final_df.groupby(['document_name', 'affiliate_payor_id']).agg({
        #     'original_document_link': 'first', 
        #     'zignaai_document_url': 'first', 
        #     'line_of_business_id': lambda x: ','.join(set(x)),
        #     'service_level_id': lambda x: ','.join(set(x)),
        #     'state_id': lambda x: ','.join(set(x)),
        #     'payor_id': 'first',
        #     'document_type_id': 'first',
        #     'effective_date': 'first', 
        #     'last_refresh_date': 'first',
        #     'keywords': 'first',
        #     'payor_policy_number': 'first'
            
        # }).reset_index()
        # final_df['service_level_id'] = final_df['service_level_id'].apply(lambda x: 'null' if x not in ('1', '2') else x)
        
        # final_df['end_date'] = ''
        # final_df['contractor_id'] = ''
        # final_df['contractor_type_id'] = ''
        # final_df['retired'] = 'N'

        # final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id',
        #                      'payor_id', 'affiliate_payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contractor_type_id', 'retired']]
        # print("shape of final df after grouping is: ", final_df.shape)

        # csv_buffer = io.BytesIO()
        # final_df.to_csv(csv_buffer, sep = '|', index=False)
        # csv_buffer.seek(0)
        # current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        # application_file_name = current_time + "_" + APPLICATION_DEST_FOLDER_LEVEL_1.lower() + "_" + str(batch_0) + "_" + str(batch_1) + ".txt"
        # application_out_filepath = f"{client}/{APPLICATION_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_3}/{application_file_name}"
        # job_type = "policy catalog"
        # comment = 'abstraction file inserted'

        # s3_client.upload_fileobj(csv_buffer, bucket, application_out_filepath)
        # print("Application Files uploaded to S3 bucket successfully")
        # api_url = f'{api}insert_task_into_queue'
        # status_code = send_payload(api_url, application_out_filepath, job_type,comment, token)
        # print(f"API hit: {status_code}")
    return "Application Files uploaded to S3 bucket successfully"