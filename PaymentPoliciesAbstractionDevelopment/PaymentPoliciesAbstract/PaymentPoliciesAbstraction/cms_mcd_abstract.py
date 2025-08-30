import pandas as pd
import re
import numpy as np
import boto3
from datetime import datetime, date
from constants import s3_client,DOWNLOAD_BUCKET_NAME
from constants import cms_mcd_ncd_service_level_dict,replace_keys_with_ids
from constants import cms_mcd_articles_path,cms_mcd_lcd_path,cms_mcd_ncd_path
from constants import get_auth_token, replace_keys_with_ids, payor_id_mapping_fn,all_ids_per_category, add_column_prefix, document_type_map, affiliate_map, states_map, lineofbusiness_map, service_level_map, contract_type_map, contractor_name_map
from constants import APPLICATION_DEST_FOLDER_LEVEL_2, APPLICATION_DEST_FOLDER_LEVEL_3, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2, RAW_DEST_FOLDER_LEVEL_4, RAW_DEST_FOLDER_LEVEL_5, RAW_DEST_FOLDER_LEVEL_6, RAW_DEST_FOLDER_LEVEL_7
from io import BytesIO
import io
import time

# custom import
from send_payload import send_payload

APPLICATION_DEST_FOLDER_LEVEL_1 = 'cms' 

def read_csv_from_s3(s3_bucket, s3_key):
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_csv(response['Body'],dtype = str,encoding_errors='ignore')
    return df

def read_articles(cms_mcd_articles_path):
    print(cms_mcd_articles_path.get('article'))
    article = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('article'))
    article_x_contractor = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('article_x_contractor'))
    contractor = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('contractor'))
    contractor_jurisdiction = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('contractor_jurisdiction'))
    contractor_type_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('contractor_type_lookup'))
    state_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('state_lookup'))
    state_x_region = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('state_x_region'))
    region_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('region_lookup'))
    dmerc_region_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('dmerc_region_lookup'))
    icd10_cov = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('icd10_cov'))
    icd10_noncov = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('icd10_noncov'))
    hcpc = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('hcpc'))
    code_table = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('code_table'))
    hcpc_mod = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('hcpc_mod'))
    icd10_pcs = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('icd10_pcs'))
    related_documents = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_articles_path.get('related_documents'))

    return article, article_x_contractor, contractor, contractor_jurisdiction, contractor_type_lookup, state_lookup, state_x_region, region_lookup, dmerc_region_lookup, icd10_cov, icd10_noncov, hcpc, code_table, hcpc_mod, icd10_pcs, related_documents

def read_lcd(cms_mcd_lcd_path):
    lcd = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('lcd'))
    lcd_x_contractor = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('lcd_x_contractor'))
    contractor = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('contractor'))
    contractor_jurisdiction = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('contractor_jurisdiction'))
    contractor_type_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('contractor_type_lookup'))
    state_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('state_lookup'))
    state_x_region = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('state_x_region'))
    region_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('region_lookup'))
    dmerc_region_lookup = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('dmerc_region_lookup'))
    hcpc = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_lcd_path.get('hcpc'))

    return lcd, lcd_x_contractor, contractor, contractor_jurisdiction, contractor_type_lookup, state_lookup, state_x_region, region_lookup, dmerc_region_lookup, hcpc

def read_ncd(cms_mcd_ncd_path):
    ncd_trkg = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_ncd_path.get('ncd_trkg'))
    ncd_trkg_bnft_xref = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_ncd_path.get('ncd_trkg_bnft_xref'))
    ncd_bnft_ctgry_ref = read_csv_from_s3(DOWNLOAD_BUCKET_NAME, cms_mcd_ncd_path.get('ncd_bnft_ctgry_ref'))

    return ncd_trkg, ncd_trkg_bnft_xref, ncd_bnft_ctgry_ref


def process_articles(cms_mcd_articles_path):
    
    article, article_x_contractor, contractor, contractor_jurisdiction, contractor_type_lookup, state_lookup, state_x_region, region_lookup, dmerc_region_lookup, icd10_cov, icd10_noncov, hcpc, code_table, hcpc_mod, icd10_pcs, related_documents = read_articles(cms_mcd_articles_path)

    t1 = pd.merge(article, article_x_contractor, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'left')
    t1 = t1[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date','contractor_id', 'contractor_type_id', 'contractor_version']].drop_duplicates()

    t2 = pd.merge(t1, contractor, left_on = ['contractor_id','contractor_type_id','contractor_version'], right_on = ['contractor_id','contractor_type_id','contractor_version'], how = 'left')
    t2 = t2[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version','contractor_bus_name', 'contractor_number']].drop_duplicates()

    t3 = pd.merge(t2, contractor_jurisdiction, left_on = ['contractor_id', 'contractor_type_id', 'contractor_version'], right_on = ['contractor_id', 'contractor_type_id', 'contractor_version'], how = 'left')
    t3 = t3[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id','term_date']].drop_duplicates()

    t4 = pd.merge(t3, contractor_type_lookup, left_on = 'contractor_type_id', right_on = 'contractor_type_id', how = 'left')
    t4.rename(columns = {'description':'contract_type'},inplace = True)
    t4 = t4[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type','term_date']].drop_duplicates()

    t5 = pd.merge(t4, state_lookup, left_on = 'state_id', right_on = 'state_id', how = 'left')
    t5.rename(columns = {'description':'states'},inplace = True)
    t5 = t5[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states','term_date']].drop_duplicates()

    t6 = pd.merge(t5, state_x_region, left_on = 'state_id', right_on = 'state_id', how = 'left')
    t6 = t6[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states', 'region_id','term_date']].drop_duplicates()

    t7 = pd.merge(t6, region_lookup, left_on = 'region_id', right_on = 'region_id', how = 'left')
    t7.rename(columns = {'description':'region'},inplace = True)
    t7 = t7[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states', 'region_id', 'region','term_date']].drop_duplicates()

    t8 = pd.merge(t7, dmerc_region_lookup, left_on = 'region_id', right_on = 'region_id', how = 'left')
    t8 = t8[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_bus_name', 'contractor_number', 'contract_type', 'states', 'region','term_date']].drop_duplicates()
  
    t8.drop(t8[t8['term_date'].notna()].index, inplace=True)
    t8.drop('term_date', axis=1, inplace=True)

    t15 = pd.merge(t8, related_documents, left_on = ['article_id', 'article_version'], right_on = ['article_id', 'article_version'], how = 'left')
    t15.rename(columns = {'r_lcd_id':'related_lcd_id', 'r_lcd_version':'related_lcd_version'},inplace = True)
    t15 = t15[['article_id', 'article_version', 'title', 'article_pub_date', 'article_eff_date', 'article_end_date', 'contractor_bus_name', 'contractor_number', 'contract_type', 'states', 'region', 'related_lcd_id', 'related_lcd_version']].drop_duplicates()

    
    #codes
    t9 = pd.merge(article, icd10_cov, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t9.rename(columns = {'icd10_code_id':'codes'},inplace = True)
    t9 = t9[['article_id', 'article_version', 'codes']]

    t10 = pd.merge(article, icd10_noncov, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t10.rename(columns = {'icd10_code_id':'codes'},inplace = True)
    t10 = t10[['article_id', 'article_version', 'codes']]

    t11 = pd.merge(article, hcpc, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t11.rename(columns = {'hcpc_code_id':'codes'},inplace = True)
    t11 = t11[['article_id', 'article_version', 'codes']]

    t12 = pd.merge(article, code_table, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t12.rename(columns = {'hcpc_code_id':'codes'},inplace = True)
    t12 = t12[['article_id', 'article_version', 'codes']]

    t13 = pd.merge(article, hcpc_mod, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t13.rename(columns = {'hcpc_modifier_code_id':'codes'},inplace = True)
    t13 = t13[['article_id', 'article_version', 'codes']]

    t14 = pd.merge(article, icd10_pcs, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'inner')
    t14.rename(columns = {'icd10_pcs_code_id':'codes'},inplace = True)
    t14 = t14[['article_id', 'article_version', 'codes']]

    concat_df = pd.concat([t9,t10,t11,t12,t13,t14], ignore_index=True)
    concat_df['codes'] = concat_df['codes'].astype(str)
    code_df = concat_df.groupby(['article_id', 'article_version'])['codes'].agg(lambda x: ', '.join(set(code for code in x))).reset_index()

    final_df = pd.merge(t15, code_df, left_on = ['article_id','article_version'], right_on = ['article_id','article_version'], how = 'left')
    
    final_df['url'] = 'https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleid='+ final_df['article_id'] + '&ver=' + final_df['article_version']
    final_df['line_of_business'] = 'Medicare'
    final_df['service_level'] = 'null'
    final_df['policy_type'] = 'CMS Articles'
    final_df['article_number'] = final_df['article_id'].apply(lambda x : 'A'+ str(x))
    
    tag_pattern = r'<[^>]+>'
    final_df['title'] = final_df['title'].apply(lambda x: re.sub(tag_pattern, '', x))
    
    # final_df['title'] = final_df['title'].apply(lambda x: x.split('/')[-1].replace('.html','').replace('%20','_'))
    pattern = re.compile('[^a-zA-Z0-9]+')
    final_df['title'] = final_df['title'].apply(lambda x: re.sub(pattern, '_', x))

    for i,j in final_df.iterrows():
        final_df.loc[i,'title'] = str(j['article_id']) + '_' + str(j['article_version']) + "_" + str(j['title'])

    final_df['article_pub_date'] = pd.to_datetime(final_df['article_pub_date'], errors='coerce')
    final_df['article_pub_date'] = final_df['article_pub_date'].dt.strftime('%Y-%m-%d')
    final_df['article_eff_date'] = pd.to_datetime(final_df['article_eff_date'], errors='coerce')  ####
    final_df['article_eff_date'] = final_df['article_eff_date'].dt.strftime('%Y-%m-%d')
    final_df['article_end_date'] = pd.to_datetime(final_df['article_end_date'], errors='coerce')
    final_df['article_end_date'] = final_df['article_end_date'].dt.strftime('%Y-%m-%d')

    final_df.rename(columns={'title':'document_name', 'article_eff_date':'effective_date', 'article_pub_date':'original_effective_date', 
                            'article_end_date':'end_date', 'contractor_bus_name':'contractor_id', 'article_number': 'payor_policy_number', 
                            'url':'original_document_link', 'line_of_business':'line_of_business_id', 'service_level':'service_level_id', 
                            'policy_type':'document_type_id', 'states':'state_id', 'contract_type':'contract_type_id'}, inplace=True)
    
    final_df['effective_date'] = final_df['effective_date'].fillna('null')
    final_df['original_effective_date'] = final_df['original_effective_date'].fillna('null')

    for i,j in final_df.iterrows():
        if j['effective_date'] == 'null':
            final_df.at[i, 'effective_date'] = j['original_effective_date']

    final_df['zignaai_document_url'] = final_df['document_name'] + ".html"
    final_df['payor_id'] = 'CMS'
    final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
    final_df['codes'] = final_df['codes'].fillna("")
    final_df['keywords'] = final_df['codes'] + ", " + final_df['payor_policy_number']
    final_df['document_name'] = 'Article_' + final_df['payor_policy_number'] + '_'+ final_df['document_name']
    final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contract_type_id']]
    
    return final_df


def process_lcd(cms_mcd_lcd_path):

    lcd, lcd_x_contractor, contractor, contractor_jurisdiction, contractor_type_lookup, state_lookup, state_x_region, region_lookup, dmerc_region_lookup, hcpc = read_lcd(cms_mcd_lcd_path)

    t1 = pd.merge(lcd, lcd_x_contractor, left_on = ['lcd_id','lcd_version'], right_on = ['lcd_id','lcd_version'], how = 'left')
    t1 = t1[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version']].drop_duplicates()

    t2 = pd.merge(t1, contractor, left_on = ['contractor_id','contractor_type_id','contractor_version'], right_on = ['contractor_id','contractor_type_id','contractor_version'], how = 'left')
    t2 = t2[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version','contractor_bus_name', 'contractor_number']].drop_duplicates()

    t3 = pd.merge(t2, contractor_jurisdiction, left_on = ['contractor_id', 'contractor_type_id', 'contractor_version'], right_on = ['contractor_id', 'contractor_type_id', 'contractor_version'], how = 'left')
    t3 = t3[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id','term_date']].drop_duplicates()

    t4 = pd.merge(t3, contractor_type_lookup, left_on = 'contractor_type_id', right_on = 'contractor_type_id', how = 'left')
    t4.rename(columns = {'description':'contract_type'},inplace = True)
    t4 = t4[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type','term_date']].drop_duplicates()

    t5 = pd.merge(t4, state_lookup, left_on = 'state_id', right_on = 'state_id', how = 'left')
    t5.rename(columns = {'description':'states'},inplace = True)
    t5 = t5[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states','term_date']].drop_duplicates()

    t6 = pd.merge(t5, state_x_region, left_on = 'state_id', right_on = 'state_id', how = 'left')
    t6 = t6[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states', 'region_id','term_date']].drop_duplicates()

    t7 = pd.merge(t6, region_lookup, left_on = 'region_id', right_on = 'region_id', how = 'left')
    t7.rename(columns = {'description':'region'},inplace = True)
    t7 = t7[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_id', 'contractor_type_id', 'contractor_version', 'contractor_bus_name', 'contractor_number', 'state_id', 'contract_type', 'states', 'region_id', 'region','term_date']].drop_duplicates()
    
    t8 = pd.merge(t7, dmerc_region_lookup, left_on = 'region_id', right_on = 'region_id', how = 'left')
    t8 = t8[['lcd_id', 'lcd_version', 'title', 'orig_det_eff_date', 'rev_eff_date', 'rev_end_date', 'mcd_publish_date', 'comment_end_dt', 'display_id', 'contractor_bus_name', 'contractor_number', 'contract_type', 'states', 'region','term_date']].drop_duplicates()
    
    t8.drop(t8[t8['term_date'].notna()].index, inplace=True)
    t8.drop('term_date', axis=1, inplace=True)
    
    #codes
    t9 = pd.merge(lcd, hcpc, left_on = ['lcd_id','lcd_version'], right_on = ['lcd_id','lcd_version'], how = 'inner')
    t9.rename(columns = {'hcpc_code_id':'codes'},inplace = True)
    t9 = t9[['lcd_id', 'lcd_version', 'codes']]
    t9['codes'] = t9['codes'].astype(str)

    code_df = t9.groupby(['lcd_id', 'lcd_version'])['codes'].agg(lambda x: ', '.join(set(code for code in x))).reset_index()

    final_df = pd.merge(t8, code_df, left_on = ['lcd_id','lcd_version'], right_on = ['lcd_id','lcd_version'], how = 'left')
    final_df['url'] = 'https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=' + final_df['lcd_id'] + '&ver=' + final_df['lcd_version']
    final_df['line_of_business'] = 'Medicare'
    final_df['service_level'] = 'null'
    final_df['policy_type'] = 'CMS LCD'
    final_df['lcd_number'] = 'L' + final_df['lcd_id']

    tag_pattern = r'<[^>]+>'
    final_df['title'] = final_df['title'].apply(lambda x: re.sub(tag_pattern, '', x))

    # final_df['title'] = final_df['title'].apply(lambda x: x.split('/')[-1].replace('.html','').replace('%20','_'))
    pattern = re.compile('[^a-zA-Z0-9]+')
    final_df['title'] = final_df['title'].apply(lambda x: re.sub(pattern, '_', x))

    for i,j in final_df.iterrows():
        final_df.loc[i,'title'] = str(j['lcd_id']) + '_' + str(j['lcd_version']) + "_" + str(j['title'])

    final_df['orig_det_eff_date'] = pd.to_datetime(final_df['orig_det_eff_date'], errors='coerce')
    final_df['orig_det_eff_date'] = final_df['orig_det_eff_date'].dt.strftime('%Y-%m-%d')
    final_df['rev_eff_date'] = pd.to_datetime(final_df['rev_eff_date'], errors='coerce') ####
    final_df['rev_eff_date'] = final_df['rev_eff_date'].dt.strftime('%Y-%m-%d')
    final_df['rev_end_date'] = pd.to_datetime(final_df['rev_end_date'], errors='coerce')
    final_df['rev_end_date'] = final_df['rev_end_date'].dt.strftime('%Y-%m-%d')
    final_df['mcd_publish_date'] = pd.to_datetime(final_df['mcd_publish_date'], errors='coerce') ####
    final_df['mcd_publish_date'] = final_df['mcd_publish_date'].dt.strftime('%Y-%m-%d')
    final_df['comment_end_dt'] = pd.to_datetime(final_df['comment_end_dt'], errors='coerce')
    final_df['comment_end_dt'] = final_df['comment_end_dt'].dt.strftime('%Y-%m-%d')

    final_df.rename(columns={'rev_eff_date':'effective_date', 'orig_det_eff_date':'original_effective_date',
                        'rev_end_date':'end_date', 'contractor_bus_name':'contractor_id', 'lcd_number': 'payor_policy_number', 
                        'url':'original_document_link', 'line_of_business':'line_of_business_id', 'service_level':'service_level_id', 
                        'policy_type':'document_type_id', 'states':'state_id', 'contract_type':'contract_type_id'}, inplace=True)
    
    final_df['effective_date'] = final_df['effective_date'].fillna('null')
    final_df['original_effective_date'] = final_df['original_effective_date'].fillna('null')
    final_df['mcd_publish_date'] = final_df['mcd_publish_date'].fillna('null')
    final_df['comment_end_dt'] = final_df['comment_end_dt'].fillna('null')

    for i,j in final_df.iterrows():
        if j['effective_date'] == 'null':
            final_df.at[i, 'effective_date'] = j['original_effective_date']

    final_df['zignaai_document_url'] = final_df['title'] + ".html"
    final_df['payor_id'] = 'CMS'
    final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
    final_df['document_name'] = 'LCD_' + final_df['payor_policy_number'] + '_'+ final_df['title']

    for i,j in final_df.iterrows():
        if (j['effective_date'] == 'null') and (j['display_id'] != ""):
            final_df.at[i, 'effective_date'] = j['mcd_publish_date']
            final_df.at[i, 'end_date'] = j['comment_end_dt']
            final_df.at[i, 'payor_policy_number'] = 'DL' + j['display_id']
            final_df.at[i, 'document_name'] = 'Proposed LCD_' + 'DL'+j['display_id'] + '_'+ j['title']
    
    final_df['codes'] = final_df['codes'].fillna("")
    final_df['keywords'] = final_df['codes'] + ", " + final_df['payor_policy_number']

    final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contract_type_id']]

    return final_df

def process_ncd(cms_mcd_ncd_path):

    ncd_trkg, ncd_trkg_bnft_xref, ncd_bnft_ctgry_ref = read_ncd(cms_mcd_ncd_path)

    t1 = pd.merge(ncd_trkg, ncd_trkg_bnft_xref, left_on = ['NCD_id', 'NCD_vrsn_num'], right_on = ['NCD_id', 'NCD_vrsn_num'], how = 'left')
    t1 = t1[['NCD_id', 'NCD_vrsn_num', 'NCD_mnl_sect', 'NCD_mnl_sect_title', 'NCD_efctv_dt', 'NCD_impltn_dt', 'NCD_trmntn_dt', 'bnft_ctgry_cd']].drop_duplicates()

    t2 = pd.merge(t1, ncd_bnft_ctgry_ref, left_on = 'bnft_ctgry_cd', right_on = 'bnft_ctgry_cd', how = 'left')
    t2 = t2[['NCD_id', 'NCD_vrsn_num', 'NCD_mnl_sect', 'NCD_mnl_sect_title', 'NCD_efctv_dt', 'NCD_impltn_dt', 'NCD_trmntn_dt', 'bnft_ctgry_cd', 'bnft_ctgry_desc']].drop_duplicates()

    t2['bnft_ctgry_desc'] = t2['bnft_ctgry_desc'].str.lower()

    t2['service_level'] = np.select([t2['bnft_ctgry_desc'].str.contains(keyword) for keyword in cms_mcd_ncd_service_level_dict.keys()],[cms_mcd_ncd_service_level_dict[keyword] for keyword in cms_mcd_ncd_service_level_dict.keys()],default='null')
    t2['url'] = 'https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid=' + t2['NCD_id'] + '&ncdver=' + t2['NCD_vrsn_num']
    t2['line_of_business'] = 'Medicare'
    t2['states'] = 'null'
    t2['policy_type'] = 'CMS NCD'

    final_df = t2.copy()
    tag_pattern = r'<[^>]+>'
    final_df['NCD_mnl_sect_title'] = final_df['NCD_mnl_sect_title'].apply(lambda x: re.sub(tag_pattern, '', x))

    # final_df['title'] = final_df['NCD_mnl_sect_title'].apply(lambda x: x.split('/')[-1].replace('.html','').replace('%20','_'))
    pattern = re.compile('[^a-zA-Z0-9]+')
    final_df['title'] = final_df['NCD_mnl_sect_title'].apply(lambda x: re.sub(pattern, '_', x))
    for i,j in final_df.iterrows():
        final_df.loc[i,'title'] = str(j['NCD_id']) + '_' + str(j['NCD_vrsn_num']) + "_" + str(j['title'])

    final_df.drop(['NCD_mnl_sect_title', 'bnft_ctgry_cd'], axis=1, inplace=True)
    final_df['NCD_efctv_dt'] = pd.to_datetime(final_df['NCD_efctv_dt'], errors='coerce')
    final_df['NCD_efctv_dt'] = final_df['NCD_efctv_dt'].dt.strftime('%Y-%m-%d')
    final_df['NCD_impltn_dt'] = pd.to_datetime(final_df['NCD_impltn_dt'], errors='coerce')
    final_df['NCD_impltn_dt'] = final_df['NCD_impltn_dt'].dt.strftime('%Y-%m-%d')
    final_df['NCD_trmntn_dt'] = pd.to_datetime(final_df['NCD_trmntn_dt'], errors='coerce')
    final_df['NCD_trmntn_dt'] = final_df['NCD_trmntn_dt'].dt.strftime('%Y-%m-%d')

    final_df.rename(columns={'title':'document_name', 'NCD_efctv_dt':'effective_date', 'NCD_trmntn_dt':'end_date',
                            'bnft_ctgry_desc':'benefit_category_description', 'NCD_mnl_sect': 'payor_policy_number', 'url':'original_document_link',
                            'line_of_business':'line_of_business_id', 'service_level':'service_level_id', 'policy_type':'document_type_id',
                            'states':'state_id'}, inplace=True)
    
    final_df['zignaai_document_url'] = final_df['document_name'] + '.html'
    final_df['payor_id'] = 'CMS'
    final_df['last_refresh_date'] = datetime.now().strftime("%Y-%m-%d")
    final_df['keywords'] = final_df['payor_policy_number']
    final_df['document_name'] = 'NCD_' + final_df['payor_policy_number'] + '_'+ final_df['document_name']
    final_df['contractor_id'] = 'null'
    final_df['contract_type_id'] = 'null'

    retired_mask = final_df['document_name'].str.contains('retired', case=False)
    print(final_df.loc[retired_mask].shape)
    final_df.loc[retired_mask & final_df['end_date'].isna(), 'end_date'] = final_df.loc[retired_mask, 'effective_date']

    final_df = final_df[['document_name', 'original_document_link', 'zignaai_document_url', 'line_of_business_id', 'service_level_id', 'state_id', 'payor_id', 'document_type_id', 'effective_date', 'end_date', 'last_refresh_date', 'keywords', 'payor_policy_number', 'contractor_id', 'contract_type_id']]
    return final_df

def process_files(batch_0, batch_1, cms_mcd_articles_path, cms_mcd_lcd_path, cms_mcd_ncd_path, BUCKET_NAME, API, AUTH_TOKEN):

    article_df = process_articles(cms_mcd_articles_path)
    lcd_df = process_lcd(cms_mcd_lcd_path)
    ncd_df = process_ncd(cms_mcd_ncd_path)

    final_df = pd.concat([article_df, lcd_df, ncd_df], ignore_index=True)
    final_df.loc[final_df['end_date'] < final_df['effective_date'], 'end_date'] = final_df['effective_date']

    def replace_retired(sentence):
        pattern = r'retired'
        return re.sub(pattern, '', sentence, flags=re.IGNORECASE)
    
    for i, r in final_df.iterrows():
        final_df.at[i, 'document_name'] = replace_retired(r['document_name'])

    final_df['retired'] = 'N'
    final_df['affiliate_payor_id'] = 'null'
    end_date_mask = (final_df['end_date'].notnull()) & (final_df['end_date'] <= final_df['last_refresh_date'])
    final_df.loc[end_date_mask, 'retired'] = 'Y'
    print(f"Before removing drafts {final_df.shape}")
    final_df = final_df[~(final_df['effective_date'].isna())]
    final_df['effective_date_length'] = final_df['effective_date'].apply(lambda x : 0 if len(str(x)) < 5 else 1)
    final_df = final_df[final_df['effective_date_length']==1]
    final_df.drop(columns = ['effective_date_length'], inplace = True)
    print(f"After removing drafts {final_df.shape}")

    raw_csv_buffer = io.BytesIO()
    final_df.to_csv(raw_csv_buffer, index=False)
    raw_csv_buffer.seek(0)
    raw_out_filename = f"{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/{RAW_DEST_FOLDER_LEVEL_7}/{batch_0}_{batch_1}_{APPLICATION_DEST_FOLDER_LEVEL_1}_{RAW_DEST_FOLDER_LEVEL_5}_policies_abstraction.csv"
    s3_client.upload_fileobj(raw_csv_buffer, BUCKET_NAME, raw_out_filename)
    print(f"Raw extract uploaded to S3 bucket {BUCKET_NAME} successfully")

    for col in ['line_of_business_id', 'service_level_id', 'state_id', 'document_type_id', 'payor_id', 'affiliate_payor_id','contractor_id', 'contract_type_id']:
        final_df[col].fillna("null", inplace=True)
        
    for tuple_value in [('line_of_business_id', lineofbusiness_map), ('service_level_id', service_level_map), ('state_id', states_map), ('document_type_id', document_type_map), ('affiliate_payor_id', affiliate_map), ('contractor_id', contractor_name_map), ('contract_type_id', contract_type_map)]:
        key_id_dict_lower = {k.lower(): v for k, v in tuple_value[1].items()}
        final_df[tuple_value[0]] = (final_df[tuple_value[0]].fillna('').str.lower().replace(key_id_dict_lower)).astype(str)
    final_df = final_df.explode('contract_type_id')
    mapping_list = [(final_df,'line_of_business_id',all_ids_per_category(API, 'getAllBusinessLines', AUTH_TOKEN)), 
                   (final_df,'service_level_id',all_ids_per_category(API, 'getAllServiceLevels', AUTH_TOKEN)), 
                   (final_df,'state_id',all_ids_per_category(API, 'getAllStates', AUTH_TOKEN)), 
                   (final_df,'document_type_id',all_ids_per_category(API, 'getAllPoliciesTypes', AUTH_TOKEN)), 
                   (final_df,'payor_id',payor_id_mapping_fn(API,AUTH_TOKEN)),
                   (final_df,'contractor_id',all_ids_per_category(API, 'getAllContractors', AUTH_TOKEN)),
                   (final_df,'contract_type_id',all_ids_per_category(API, 'getAllContractTypes', AUTH_TOKEN))]
    for mapper in mapping_list:
        final_df = replace_keys_with_ids(mapper[0],mapper[1],mapper[2])
    print('id mapping successful')
    final_df_grouped= final_df.groupby(['document_name']).agg({
        'original_document_link': 'first',  
        'zignaai_document_url': 'first', 
        'line_of_business_id': 'first',
        'service_level_id': lambda x: ','.join(set(x)),
        'state_id': lambda x: ','.join(set(x)),
        'payor_id': 'first',
        'affiliate_payor_id':'first',
        'document_type_id': lambda x: ','.join(set(x)),
        'effective_date': 'first',
        'end_date':'first',
        'last_refresh_date': 'first',
        'keywords': 'first',
        'payor_policy_number': 'first',
        'contractor_id':lambda x: ','.join(set(x)),
        'contract_type_id': lambda x: ','.join(set(x)),
        'retired':'first'
    }).reset_index()
    final_df_grouped['service_level_id'] = final_df_grouped['service_level_id'].apply(lambda x: 'null' if x not in ('1', '2') else x)

    return final_df_grouped

def save_file_to_s3(batch_0,batch_1, bucket, api, env_secret, client, out_filename=None):
    print(bucket, api, env_secret, client)
    if (bucket != '') and (api != ''):
        auth_tok = get_auth_token(env_secret)
        final_df = process_files(batch_0, batch_1, cms_mcd_articles_path, cms_mcd_lcd_path, cms_mcd_ncd_path, bucket, api, auth_tok)
        final_df['document_name'] = final_df['document_name'].apply(lambda x: x.split("_")[0] + " -"+ x.split("_")[1]+ " -"+ " ".join(x.split("_")[4:]))
        time.sleep(8 * 60)

        batch_size = 800
        batches = [final_df[i:i+batch_size] for i in range(0, len(final_df), batch_size)]

        # Store each batch in S3
        for i, btch in enumerate(batches):
            csv_buffer = BytesIO()
            btch.to_csv(csv_buffer, index=False, sep="|")
            csv_buffer.seek(0)

            current_time = datetime.now().strftime("%Y_%m_%d_%H%M%S")
            payor_id = btch['payor_id'].unique()[0]
            application_file_name = current_time + "_" + APPLICATION_DEST_FOLDER_LEVEL_1.lower() + "_" + str(i) + ".txt"
            application_out_filepath = f"{client}/{APPLICATION_DEST_FOLDER_LEVEL_2}/{APPLICATION_DEST_FOLDER_LEVEL_3}/{application_file_name}"
            print(application_out_filepath)

            job_type = "policy catalog"
            comment = 'abstraction file inserted'

            s3_client.upload_fileobj(csv_buffer, bucket, application_out_filepath)
            print(f"Application Files uploaded to S3 bucket {bucket} successfully")
            api_url = f'{api}insert_task_into_queue'
            status_code = send_payload(api_url, application_file_name, job_type,comment, auth_tok)
            print(f"API hit: {status_code}")
    return "Files uploaded to S3 bucket"