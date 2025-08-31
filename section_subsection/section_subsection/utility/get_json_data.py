import os
import json
import pandas as pd
from io import StringIO
from datetime import datetime

try:
    from get_json import read_json
    from get_drg_type import get_drg_type
    from utils import send_payload, get_bucket_api, get_auth_token
except:
    from .get_json import read_json
    from .get_drg_type import get_drg_type
    from .utils import send_payload, get_bucket_api, get_auth_token
class JsonData:
    def __init__(self) -> None:
        pass

    @staticmethod
    def copy_object_to_s3(s3r, bucket, copy_source, copy_key, api_endpoint_url, mr_name):
        s3r.copy_object(CopySource=copy_source, Bucket=bucket,
                        Key=copy_key)
        print(f'copied {copy_source} to the key:  {copy_key}')
        secret_path = os.environ['SECRET_PATH']
        access_token = get_auth_token(secret_path)
        # status_code = send_payload(
        #     api_endpoint_url, copy_key, access_token, mr_name, job_type='auditDrg_sections', comments='mr section inserted')
        # print(status_code)
        return 'copied'

    @staticmethod
    def get_chart_view_json_data(df, s3_c, bucket_name, path_to_save_result, file_name, parameters_bucket_name, grd_truth_const_path, client_name, mr_name, zai_audit_process_params_key):
        file_name_ = file_name.replace('.csv', '')
        json_file_name = f"{file_name_}_mr_section_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.json"
        claim_id = file_name_.split('_')[0]
        data = []
        chart_order_ls = ['Demographics (DEMO)', 'Emergency Department (ED)', 'History and Physical (H&P)',
                 'Progress Notes including Consults (Physician/QHP)', 'Operative/Procedure Note',
                 'Discharge Summary (DS)', 'Therapy Notes', 'Dietary/Nutritional Notes', 'Nursing Documentation',
                 'LABS', 'Imaging', 'Orders', 'Miscellaneous']
        for chart in chart_order_ls:
            section_data_ = []
            section_data = []
            sub_df = df[df['chart_order'] == chart]
            if len(sub_df) > 0:
                for i, row in sub_df.iterrows():
                    section_data_.append({'QHP': row['Physician Name'],
                                          'section': row['entity_actual'],
                                          'page_link': f"{file_name_.replace(claim_id+'_', '')}_highlighted.pdf#page={eval(row['range'])[0]}",
                                          'service_date': str(row['group_date']),
                                          'document_name': f"{file_name_.replace(claim_id+'_', '')}_highlighted",
                                          'start': eval(row['range'])[0],
                                          'end': eval(row['range'])[1],
                                          })
                    # section_data.append({'data':section_data_})
                data.append({'data': section_data_, 'section': chart})
        
        json_data = {'claim_id': f"{claim_id}", 'section_data': data}
        detected_chart = df['chart_order'].unique().tolist()
        drg_type = get_drg_type(s3_c, parameters_bucket_name, grd_truth_const_path, file_name)
        if drg_type and drg_type == 'SURG':
            req_chart = ['History and Physical (H&P)', 'Progress Notes including Consults (Physician/QHP)', 'Operative/Procedure Note']
        else:
            req_chart = ['History and Physical (H&P)', 'Progress Notes including Consults (Physician/QHP)']


        # --------------- local save the data ---------------
        # with open(rf"{path_to_save_result}\{json_file_name}", 'w') as f:
        #     json.dump(json_data, f, indent=2)
        # --------------- end ---------------
        put_resp_flag = s3_c.put_object(Bucket=bucket_name, Body=json.dumps(json_data),
                                        Key=rf'{path_to_save_result}/{json_file_name}')
        
        # copy files for UI
        copy_source = {'Bucket': bucket_name,
                       'Key': f'{path_to_save_result}/{json_file_name}'}
        # --------------- lambda ---------------
        destination_bucket, api_endpoint_url, save_path, _ =  get_bucket_api(s3_c, file_name_, parameters_bucket_name, client_name, zai_audit_process_params_key)
        copy_key = f"{save_path}/{claim_id}/{json_file_name.replace(claim_id+'_', '')}"
        print(f'{copy_source} to---> {copy_key}')
        JsonData.copy_object_to_s3(
            s3_c, destination_bucket, copy_source, copy_key, api_endpoint_url, mr_name)
        print(f'copy_source: { copy_source} | copy_key: {copy_key} ')
        print(f'final json data stored for {file_name}')
        # if len(req_chart) == len(set(detected_chart).intersection(set(req_chart))):
        #     status_code = send_payload(
        #         api_endpoint_url, copy_key, job_type='auditDrg_sections', comments='mr section inserted')
        #     print(status_code)

        # else:
        #     status_code = send_payload(
        #         api_endpoint_url, copy_key, job_type='auditDrg_sections', comments='mr_incomplete')
        #     print(status_code)

        # --------------- end ---------------
        return json_data
