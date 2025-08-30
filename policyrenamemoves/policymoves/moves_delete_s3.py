import pandas as pd
import boto3
import os
from io import StringIO
import io
from datetime import date, datetime
from constants import s3_client
from send_payload import send_payload
from constants import SOURCE_FOLDER_LEVEL_1, SOURCE_FOLDER_LEVEL_2, SOURCE_FOLDER_LEVEL_4, SOURCE_FOLDER_LEVEL_5, SOURCE_FOLDER_LEVEL_6, SOURCE_FOLDER_LEVEL_7, DEST_FOLDER_LEVEL_1, DEST_FOLDER_LEVEL_2, DEST_FOLDER_LEVEL_4, DEST_FOLDER_LEVEL_5, FINAL_FILE_FOLDER_LEVEL_2, FINAL_FILE_FOLDER_LEVEL_3

def get_matching_s3_keys(s3, bucket, prefix):
    flag = False
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    for page in paginator.paginate(**kwargs):
        try:
            contents = page["Contents"]
            if contents:
                flag = True
        except Exception as e:
            print('Error: - ', e)
            break
    return flag

def copy_object_to_s3(s3r, bucket, copy_source, copy_key ):
    try:
        s3r.copy_object(CopySource=copy_source, Bucket=bucket,
                        Key=copy_key)
        print(f'copied {copy_source} to the key:  {copy_key}')
        return 'copied'
    except Exception as e:
        print(f"Error in copying: {e}, Policy not copied")
        return 'not copied'

def move_delete_policies(payor_id, payername, df, key, bucket_name, api, token, client, moves_file_name):
    copied_ls = []
    not_copied_ls = []
    attribute_list = []  
    temp = {"payor_id": payor_id,
            "last_refresh_date": datetime.now().strftime("%Y-%m-%d")}
    
    try:
        columns = df.columns.to_list()
        if ("last_refresh_date" in columns) and ("new_file_name" in columns) and ("current_file_name" in columns) and ((df.shape[0])>0):
            for j in range(df.shape[0]):
                current_file_name = df.iloc[j]['current_file_name']
                new_file_name = df.iloc[j]['new_file_name']

                copy_source = {'Bucket': bucket_name, 'Key': f'{SOURCE_FOLDER_LEVEL_1}/{SOURCE_FOLDER_LEVEL_2}/{payername}/{SOURCE_FOLDER_LEVEL_4}/{SOURCE_FOLDER_LEVEL_5}/{SOURCE_FOLDER_LEVEL_6}/{SOURCE_FOLDER_LEVEL_7}/{current_file_name}'}
                copy_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{payername}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{new_file_name}'

                Prefix = f'{SOURCE_FOLDER_LEVEL_1}/{SOURCE_FOLDER_LEVEL_2}/{payername}/{SOURCE_FOLDER_LEVEL_4}/{SOURCE_FOLDER_LEVEL_5}/{SOURCE_FOLDER_LEVEL_6}/{SOURCE_FOLDER_LEVEL_7}/{current_file_name}'
                flag_value = get_matching_s3_keys(s3_client, bucket_name, Prefix)
                if flag_value == True:
                    try:
                        status = copy_object_to_s3(s3_client, bucket_name, copy_source, copy_key)
                        print(status)
                        if status == 'copied':
                            copied_ls.append(current_file_name)
                        else:
                            not_copied_ls.append(current_file_name)
                    except:
                        print('Copy Failed')
                        not_copied_ls.append(current_file_name)
                else:
                    print("Key/Policy not found in the s3 bucket")
                    not_copied_ls.append(current_file_name)

            copied_df = pd.DataFrame(copied_ls)
            not_copied_df = pd.DataFrame(not_copied_ls)

            copied_csv_buffer = io.BytesIO()
            copied_df.to_csv(copied_csv_buffer, index=False)
            copied_csv_buffer.seek(0)
            copied_filename = f'{SOURCE_FOLDER_LEVEL_1}/{SOURCE_FOLDER_LEVEL_2}/{payername}/{SOURCE_FOLDER_LEVEL_4}/{SOURCE_FOLDER_LEVEL_5}/{SOURCE_FOLDER_LEVEL_6}/{moves_file_name}_copied_policy_names.csv'
            s3_client.upload_fileobj(copied_csv_buffer, bucket_name, copied_filename)
            print(f"Copied policy list uploaded to S3 bucket {bucket_name} successfully")

            not_copied_csv_buffer = io.BytesIO()
            not_copied_df.to_csv(not_copied_csv_buffer, index=False)
            not_copied_csv_buffer.seek(0)
            not_copied_filename = f'{SOURCE_FOLDER_LEVEL_1}/{SOURCE_FOLDER_LEVEL_2}/{payername}/{SOURCE_FOLDER_LEVEL_4}/{SOURCE_FOLDER_LEVEL_5}/{SOURCE_FOLDER_LEVEL_6}/{moves_file_name}_not_copied_policy_names.csv'
            s3_client.upload_fileobj(not_copied_csv_buffer, bucket_name, not_copied_filename)
            print(f"Not copied policy list uploaded to S3 bucket {bucket_name} successfully")

            attribute_list.append(temp)
            final_df = pd.DataFrame(attribute_list)
            csv_buffer = io.BytesIO()
            final_df.to_csv(csv_buffer, sep = "|", index=False)
            csv_buffer.seek(0)
            out_filename = datetime.now().strftime("%Y_%m_%d_%H%M%S") + '_' + payername + '_final.txt'
                
            out_filepath = f'{client}/{FINAL_FILE_FOLDER_LEVEL_2}/{FINAL_FILE_FOLDER_LEVEL_3}/{out_filename}'
            s3_client.upload_fileobj(csv_buffer, bucket_name, out_filepath)
            print(f"Final df uploaded to S3 bucket successfully in {out_filepath}") 

            # posting data to API
            api_url = f"{api}insert_task_into_queue"
            job_type = "policy_catalog_final"
            comment = "final file inserted"
            status_code = send_payload(api_url, out_filename, job_type,comment, token)
            print(f"API hit: {status_code}")
        return "copied, not_copied lists and final df is inserted"
    except Exception as e:
        print(key,"->>>> except ", e)
        pass
