from datetime import datetime,date,timedelta
import pandas as pd
import os
import re
from io import BytesIO
from pdfminer.high_level import extract_text
from devoted_attributes_extraction import *
from constants import  date_formats, service_level_dict, add_column_prefix, COLUMN_PREFIX,s3_client
from constants import get_auth_token, replace_keys_with_ids, payor_id_mapping_fn,all_ids_per_category, add_column_prefix, document_type_map, affiliate_map, states_map, lineofbusiness_map, service_level_map, contract_type_map, contractor_name_map
from constants import APPLICATION_DEST_FOLDER_LEVEL_2, APPLICATION_DEST_FOLDER_LEVEL_3, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2, RAW_DEST_FOLDER_LEVEL_4, RAW_DEST_FOLDER_LEVEL_5, RAW_DEST_FOLDER_LEVEL_6, RAW_DEST_FOLDER_LEVEL_7
import sys
import traceback

# custom import
from send_payload import send_payload

APPLICATION_DEST_FOLDER_LEVEL_1 = 'devoted'

def read_pdf(file_path):
    try:
        with open(file_path, 'rb') as f:
            pdf_byte = f.read()
        text = extract_text(BytesIO(pdf_byte))
        return text
    except Exception as e:
        print("Error: ", e)
        text = ''
        return text

def assign_effective_date(dates, date_formats):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
    date_objects = []
    for date in dates:
        for pattern, date_format in date_formats.items():
                if re.match(pattern, date):
                    date_objects.append(datetime.strptime(date, date_format))
                    effective_date = max(date_objects).strftime(date_format)
                    oldest_date = min(date_objects).strftime(date_format)
                    break
                else:
                    effective_date = None
    return effective_date

def get_effective_date_history(text, section_name):
    lines = text.split("\n")
    df = section_cleanup(text)
    df3 = section_range(df)
    if len(df3) != 0:
        if section_name in df3['section'].to_list():
            section_text = extract_text_for_section(df3, section_name, lines)
            sec_text_joined = "\n".join(section_text)
            fp_dates_ex_text = dates_from_text(sec_text_joined)
            dates = re.findall(r"\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{2,4}", fp_dates_ex_text) or re.findall(r"\d{1,4}[-./\, _:|]\d{1,4}", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?", fp_dates_ex_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b",
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", fp_dates_ex_text) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b", fp_dates_ex_text)
            if len(dates)>1:
                effective_date = assign_effective_date(dates, date_formats)
            elif len(dates)==1:
                effective_date = dates[0]
            else:
                effective_date = nt_check_effective_date(text)
        else:
            effective_date = nt_check_effective_date(text)
    else:
        effective_date = 'NA'
    return effective_date

def get_effective_date(text):
    try:
        effective_date = 'NA'
        effective_date_patterns = ['Policy approval and history:', 'History','Policy History/Revision Information','Guidline History/Revision Information']
        effective_date = check_effective_date(text)
        if effective_date == 'NA':
            for pattern in effective_date_patterns:
                effective_date = get_effective_date_history(text, pattern)
                break
    except Exception as e:
        print("Error :- ", e)
        effective_date = 'NA'
    return effective_date

def get_list_of_codes(text):
    codes_list = []
    code_range_list = []
    final_codes = []
    try:
        lines = text.split('\n')
        code_range_list = code_range_extraction(lines)
        codes_list = codes_extraction_with_type(lines)
        # print(code_range_list)
        # print(codes_list)
        final_codes = code_range_list + codes_list
        final_codes = list(set(final_codes))
    except Exception as e:
        print("Error :- ", e)
    return final_codes

def get_service_level(text):
    lines = text.split("\n")
    sl_values = []
    for line in lines:
        final_line = (re.sub(r'[^a-zA-Z0-9\s]+', '', line)).lower()
        final_line = re.sub(' +', ' ', final_line)
        for sl in service_level_dict.keys():
            if sl in final_line:
                sl_values.append(service_level_dict[sl])
    sl_value = list(set(sl_values))
    if len(sl_value) == 0:
        sl_value = 'NA'
    return sl_value

function_dict = {
    'effective_date': get_effective_date,
    'list_of_codes': get_list_of_codes,
    'service_level' : get_service_level
}

def extract_attributes(df, bucket, api, env_secret):
    print(bucket, api, env_secret)
    if (bucket != '') and (api != ''):
        auth_tok = get_auth_token(env_secret)
        attribute_list = []
        print(f'Length of input dataframe {len(df)}')
        for i, j in df.iterrows():
            temp = {
                    'effective_date': '',
                    'list_of_codes': '',
                    'service_level' : ''
                    }
            if j['dwnld_status_code'] == 200:
                try:
                    uri = j['dwnld_s3_uri'].replace(f's3://{bucket}/','')
                    pdf_byte,text = read_pdf_from_s3(bucket, uri)
                    for attributes in temp.keys():
                        temp[attributes] = function_dict[attributes](text)      
                except Exception as e:
                    print("Error :- ", e)
                    exception_type, exception_value, exception_traceback = sys.exc_info()
                    line_number = traceback.extract_tb(exception_traceback)[-1][1]
                    print(f"Error :- {e} \n Line Number :- {line_number} \n Exception Type :- {exception_type}")
            temp['object_key'] = j['dwnld_s3_uri']
            temp['pdf_name'] = j['dwnld_file_name'].split('.')[0]
            temp['filename'] = j['extrctn_file_name']
            temp['state'] = j['extrctn_state']
            temp['pdf_links'] = j['extrctn_pdf_links']
            temp['client_name'] = j['extrctn_client_name']
            temp['line_of_business'] = j['extrctn_line_of_business']
            temp['policy_type'] = j['extrctn_policy_type']
            print(f'metadata dictionary {len(temp)}')
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
    if (bucket != '') and (api != ''):
        df1 = pd.DataFrame(attributes_list)
        cols = add_column_prefix(df1, COLUMN_PREFIX)
        df1.columns = cols

        raw_csv_buffer = io.BytesIO()
        df1.to_csv(raw_csv_buffer, index=False)
        raw_csv_buffer.seek(0)
        raw_out_filename = f"{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/{RAW_DEST_FOLDER_LEVEL_7}/{batch_0}_{batch_1}_{APPLICATION_DEST_FOLDER_LEVEL_1}_{RAW_DEST_FOLDER_LEVEL_5}_policies_abstraction.csv"

        s3_client.upload_fileobj(raw_csv_buffer, bucket, raw_out_filename)
        print(f"Raw extract uploaded to S3 bucket {bucket} successfully")

        df1.rename(columns={'abstrctn_filename':'document_name', 'abstrctn_effective_date':'effective_date',
                            'abstrctn_list_of_codes':'keywords', 'abstrctn_service_level':'service_level_id',
                            'abstrctn_client_name':'payor_id', 'abstrctn_pdf_links':'original_document_link', 'abstrctn_state':'state_id',
                            'abstrctn_line_of_business':'line_of_business_id', 'abstrctn_policy_type':'document_type_id'}, inplace=True)
        
        # df1['effective_date'] = pd.to_datetime(df1['effective_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df1['cleaned_effective_date'] = pd.to_datetime(df1['effective_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        df1.drop(columns=['effective_date'],inplace = True)
        df1.rename(columns={'cleaned_effective_date':'effective_date'},inplace=True)

        unique_date_count = df1['effective_date'].nunique()
        unique_dates = df1['effective_date'].unique()
        print(f'Unique Date count : {unique_date_count}')
        print(f'Unique Dates : {unique_dates}')


        df1['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
        df1['payor_policy_number'] = 'null'
        df1['zignaai_document_url'] = df1['abstrctn_pdf_name'].apply(lambda x : x + '.pdf')

        df1['state_id'] = df1['state_id'].apply(lambda x: eval(x))
        df1['keywords'] = df1['keywords'].apply(lambda lst: ', '.join(lst))
        df1['keywords'] = df1['keywords'] + ', ' + df1['abstrctn_pdf_name']
        df1['affiliate_payor_id'] = 'null'

        df1 = df1[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id',
                    'affiliate_payor_id', 'document_type_id', 'effective_date', 'last_refresh_date', 'keywords', 'payor_policy_number']]

        df1 = df1.explode('state_id')
        df1 = df1.explode('service_level_id')

        for col in ['line_of_business_id', 'service_level_id', 'state_id', 'document_type_id', 'payor_id']:
            df1.fillna({col:"null"}, inplace=True)
        
        for tuple_value in [('line_of_business_id', lineofbusiness_map), ('service_level_id', service_level_map), ('state_id', states_map), ('document_type_id', document_type_map)]:
            key_id_dict_lower = {k.lower(): v for k, v in tuple_value[1].items()}
            df1[tuple_value[0]] = (df1[tuple_value[0]].str.lower().replace(key_id_dict_lower)).astype(str)

        mapping_list = [(df1,'line_of_business_id',all_ids_per_category(api, 'getAllBusinessLines', token)),
                    (df1,'service_level_id',all_ids_per_category(api, 'getAllServiceLevels', token)),
                    (df1,'state_id',all_ids_per_category(api, 'getAllStates', token)),
                    (df1,'document_type_id',all_ids_per_category(api, 'getAllPoliciesTypes', token)),
                    (df1,'payor_id',payor_id_mapping_fn(api, token))]
        for mapper in mapping_list:
            df1 = replace_keys_with_ids(mapper[0],mapper[1],mapper[2])
        print('id mapping successful')
        final_df= df1.groupby(['document_name']).agg({
            'original_document_link': 'first', 
            'zignaai_document_url': 'first', 
            'line_of_business_id': 'first',
            'service_level_id': lambda x: ','.join(set(x)),
            'state_id': lambda x: ','.join(set(x)),
            'payor_id': 'first',
            'affiliate_payor_id':'first',
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

        final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id', 'affiliate_payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contractor_type_id', 'retired']]

        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False,sep='|')
        csv_buffer.seek(0)
        current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        payor_id = final_df['payor_id'].unique()[0]
        application_file_name = current_time + "_" + APPLICATION_DEST_FOLDER_LEVEL_1.lower() + ".txt"
        application_out_filepath = f"{client}/{APPLICATION_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_3}/{application_file_name}"

        job_type = "policy catalog"
        comment = 'abstraction file inserted'

        s3_client.upload_fileobj(csv_buffer, bucket, application_out_filepath)
        print(f"Application Files uploaded to S3 bucket {bucket} successfully")
        api_url = f'{api}insert_task_into_queue'
        status_code = send_payload(api_url, application_file_name, job_type,comment, token)
        print(f"API hit: {status_code}")
    return "Application Files uploaded to S3 bucket"