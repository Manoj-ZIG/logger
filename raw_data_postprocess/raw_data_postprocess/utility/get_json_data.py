import json
from datetime import datetime

class JsonData:
    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    
    def get_table_json_data(self,claim_id, postprocess_table_data, path_to_save_result):
        dt = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        json_file_name = f"{path_to_save_result.split('/')[-1]}_test_administered_{dt}.json"
        data = []
        default_abnormal = 0
        if postprocess_table_data.shape[0]>0:
            for test in postprocess_table_data['TestName'].unique():
                test_title_data = []
                sub_df = postprocess_table_data[postprocess_table_data['TestName']
                                                == test]
                if test == 'Baseline Serum Creatinine':
                    sub_df['TestDateTime'] = ""
                for i, row in sub_df.iterrows():
                    if row['Section']:
                        # filter_section = list(filter(lambda x: x if str(x) not in [
                        #                     'nan', 'None'] else '', eval(str(row['Section']))))
                        filter_section = str(row['Section'])
                    else:
                        filter_section = ''
                    if row['SubSection']:
                        # filter_subsection = list(filter(lambda x: x if str(x) not in [
                        #     'nan', 'None'] else '', eval(str(row['SubSection']))))
                        filter_subsection = str(row['SubSection'])
                    else:
                        filter_subsection = ''

                    if row['TestResult'] and str(row['TestResult']).lower() not in ['nan', 'none'] and str(row['TestDateTime']).lower() not in ['nan', 'none']:
                        test_title_data.append({'date': row['TestDateTime'],
                                                'value': row['TestResult'],
                                                'units': row['TestUnit'],
                                                'text': '...',
                                                "isAbnormal": int(row['IsAbnormal']) if not str(row['IsAbnormal']) in ['None', 'nan'] else default_abnormal,
                                                # "referenceRange": row['ReferenceRange'] if not str(row['ReferenceRange']) in ['None', 'nan'] else '',
                                                "section": filter_section,
                                                "subSection": filter_subsection})
                data.append({'test': test,
                            'data': test_title_data})
                
            if ('Baseline Serum Creatinine' not in postprocess_table_data['TestName'].unique()) and \
                ('Serum Creatinine' in postprocess_table_data['TestName'].unique()):
                test_title_data = []
                test_title_data.append({'date': "",
                                        'value': 1,
                                        'units': 'mg/dl',
                                        'text': '...',
                                        "isAbnormal": 0,
                                        # "referenceRange": row['ReferenceRange'] if not str(row['ReferenceRange']) in ['None', 'nan'] else '',
                                        "section": "",
                                        "subSection": ""})
                data.append({'test': "Labmin Universal",
                            'data': test_title_data})

        json_data = {"claim_id": claim_id,
                    #  "template_name": template_name,
                     "supporting_data": data}
        # save the data
        # with open(rf"{path_to_save_result}\{json_file_name}", 'w') as f:
        #     json.dump(json_data, f, indent=4)

        self.s3_client.put_object(Bucket=self.bucket_name, Body=json.dumps(json_data),
                                  Key=rf'{path_to_save_result}/{json_file_name}')
        source_key = {'Bucket': self.bucket_name,
                      'Key': rf'{path_to_save_result}/{json_file_name}'}
        return json_data, source_key

    def get_excerpt_json_data(self, claim_id, excerpt_postprocess_df, path_to_save_result, s3_url):
        dt = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        json_file_name = f"{path_to_save_result.split('/')[-1]}_excerpts_{dt}.json"
        data = []
        if excerpt_postprocess_df.shape[0]>0:
            for attr_title in excerpt_postprocess_df['TestName'].unique():
                attr_title_data = []
                sub_df = excerpt_postprocess_df[excerpt_postprocess_df['TestName']
                                                == attr_title][['Text', 'TestDateTime', 'Page', 'DocumentName']]
                for i, row in sub_df.iterrows():
                    # doc_link = f"{str(row['DocumentName']).replace('.csv','')}_highlighted.pdf#page={row['Page']}"
                    doc_link = f"{s3_url}#page={row['Page']}"
                    date = str(row['TestDateTime'])
                    date_ = "" if date in ['nan', 'none'] else date
                    attr_title_data.append({'date': date_,
                                            'text': row['Text'],
                                            # 'link': "https://www.ncbi.nlm.nih.gov/pubmed/31978945"})
                                            'link': doc_link})
                data.append({'title': attr_title,
                            'data': attr_title_data})
        else:
            print(f'postprocess_excerpt_df has shape {excerpt_postprocess_df.shape}')
        json_data = {"claim_id": claim_id,
                    #  "template_name": template_name,
                     "supporting_data": data}

        # save the data
        # with open(rf"{path_to_save_result}\{json_file_name}", 'w') as f:
        #     json.dump(json_data, f, indent=4)
        self.s3_client.put_object(Bucket=self.bucket_name, Body=json.dumps(json_data),
                                  Key=rf'{path_to_save_result}/{json_file_name}')
        source_key = {'Bucket': self.bucket_name,
                      'Key': rf'{path_to_save_result}/{json_file_name}'}
        
        return json_data, source_key


    def get_template_selection_population_data(self, claim_id, analysis_json_data, path_to_save_result, file):
        dt = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        json_file_name = f"{file.replace('.csv','')}_template_selection_{dt}.json"
        terms_to_populate = []
        for k, v in analysis_json_data.items():
            if isinstance(v, dict) and (v.get('pneumonia') == 1 or v.get('aki') == 1 or v.get('atn') == 1 or v.get('ami')==1 or v.get('sepsis')==1 or v.get('vital')==1 or v.get('encephalopathy') == 1):
                terms_to_populate.append(k)
        selectedItems = list(set(terms_to_populate))
        return_dict = {
            "claim_id": claim_id,
            # "template_name": template_name,
            "selectedItems": selectedItems if len(selectedItems) > 0 else []
        }
        # save the data
        # with open(rf"{path_to_save_result}\{json_file_name}", 'w') as f:
        #     json.dump(return_dict, f, indent=4)
        self.s3_client.put_object(Bucket=self.bucket_name, Body=json.dumps(return_dict),
                                  Key=rf'{path_to_save_result}/{json_file_name}')

        source_key = {'Bucket': self.bucket_name,
                      'Key': rf'{path_to_save_result}/{json_file_name}'}
        return return_dict, source_key
