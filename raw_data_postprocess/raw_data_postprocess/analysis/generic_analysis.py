import pandas as pd
import json
from datetime import timedelta


class GenericAnalysis:
    def __init__(self, postprocess_data, post_process_excerpt_df, test_name_range_dict, all_attribute_map_dict, vital_regex_list):
        self.postprocess_data = postprocess_data
        self.post_process_excerpt_df = post_process_excerpt_df
        self.test_name_range_dict = test_name_range_dict
        self.all_attribute_map_dict = all_attribute_map_dict
        self.vital_test_list = vital_regex_list

    @staticmethod
    def get_ref_range_and_boolean_flag(test_name, test_result, test_name_range_dict):
        if test_result:
            test_result = float(test_result)
            try:
                if not (test_name_range_dict.get(test_name).get('normal_range')[1] == None or test_name_range_dict.get(test_name).get('normal_range')[0] == None):
                    if test_result > test_name_range_dict.get(test_name).get('normal_range')[1] or test_result < test_name_range_dict.get(test_name).get('normal_range')[0]:
                        return test_name_range_dict.get(test_name).get('normal_range'), 1
                    else:
                        return test_name_range_dict.get(test_name).get('normal_range'), 0
                else:
                    if test_name_range_dict.get(test_name).get('normal_range')[1]:
                        if test_result > test_name_range_dict.get(test_name).get('normal_range')[1]:
                            return test_name_range_dict.get(test_name).get('normal_range'), 1
                        else:
                            return test_name_range_dict.get(test_name).get('normal_range'), 0
                    elif test_name_range_dict.get(test_name).get('normal_range')[0]:
                        if test_result < test_name_range_dict.get(test_name).get('normal_range')[0]:
                            return test_name_range_dict.get(test_name).get('normal_range'), 1
                        else:
                            return test_name_range_dict.get(test_name).get('normal_range'), 0
            except TypeError as e:
                if not (test_name_range_dict.get(test_name).get('pneumonia')[1] == None or test_name_range_dict.get(test_name).get('pneumonia')[0] == None):
                    if test_result > test_name_range_dict.get(test_name).get('pneumonia')[1] or test_result < test_name_range_dict.get(test_name).get('pneumonia')[0]:
                        return test_name_range_dict.get(test_name).get('pneumonia'), 1
                    else:
                        return test_name_range_dict.get(test_name).get('pneumonia'), 0
                else:
                    if test_name_range_dict.get(test_name).get('pneumonia')[1]:
                        if test_result > test_name_range_dict.get(test_name).get('pneumonia')[1]:
                            return test_name_range_dict.get(test_name).get('pneumonia'), 1
                        else:
                            return test_name_range_dict.get(test_name).get('pneumonia'), 0
                    elif test_name_range_dict.get(test_name).get('pneumonia')[0]:
                        if test_result < test_name_range_dict.get(test_name).get('pneumonia')[0]:
                            return test_name_range_dict.get(test_name).get('pneumonia'), 1
                        else:
                            return test_name_range_dict.get(test_name).get('pneumonia'), 0
        else:
            try:
                return test_name_range_dict.get(test_name).get('normal_range'), 0
            except TypeError as e:
                return test_name_range_dict.get(test_name).get('pneumonia'), 0

    def get_creatine_boolean(self):
        if self.postprocess_data.shape[0] > 0:
            # Filter for serum creatinine tests
            creatinine_df = self.postprocess_data[self.postprocess_data['TestName']
                                                == 'Serum Creatinine'].copy()
            # Filter for baseline serum creatinine tests
            baseline_serum_creatinine_df = self.postprocess_data[self.postprocess_data['TestName']
                                                == 'Baseline Serum Creatinine'].copy()
            
            creatinine_df = creatinine_df.dropna(subset=['TestResult'])
            baseline_serum_creatinine_df = baseline_serum_creatinine_df.dropna(subset=['TestResult'])
            
            creatinine_df = creatinine_df[creatinine_df['TestDateTime'] != ''].reset_index(drop=True)
            # baseline_serum_creatinine_df = baseline_serum_creatinine_df[baseline_serum_creatinine_df['TestDateTime'] != ''].reset_index(drop=True)
    
            labmin_48_hours_flag, baseline_serum_creatinine_flag, labmin_universal_flag = 0, 0, 0
            labmin_universal = 1
            
            # Initialize a list to store significant increases
            significant_increases = []

            if creatinine_df.shape[0] > 0:
                return_dict = {}

                test_result_list = creatinine_df['TestResult'].to_list()
                test_result_list_ = []
                try:
                    test_result_list_ = [float(i)
                                         for i in test_result_list if i]
                except ValueError as e:
                    pass
                
                flag_min = min(test_result_list_) < self.test_name_range_dict.get("Serum Creatinine").get('normal_range')[0]
                flag_max = max(test_result_list_) > self.test_name_range_dict.get("Serum Creatinine").get('normal_range')[1]
                return_dict["Serum Creatinine"] = {'aki': int(
                    any((flag_min, flag_max))), 'atn': 0}
                # Case 1 48 hours
                # Ensure datetime column is in datetime format
                creatinine_df['TestDateTime'] = pd.to_datetime(creatinine_df['TestDateTime'])
                
                # Sort by datetime
                creatinine_df.sort_values(by='TestDateTime', inplace=True)
                
                # Iterate through each row
                for i, row in creatinine_df.iterrows():
                    base_date_time = row['TestDateTime']
                    base_result = row['TestResult']

                    # Find tests within 48 hours after the current test
                    window_df = creatinine_df[(creatinine_df['TestDateTime'] > base_date_time) & 
                                            (creatinine_df['TestDateTime'] <= base_date_time + timedelta(hours=48))]

                    # Check for significant increase
                    for _, future_row in window_df.iterrows():
                        if future_row['TestResult'] - base_result >= 0.3:
                            labmin_48_hours_flag = 1
                            significant_increases.append({
                                'base_datetime': base_date_time,
                                'base_result': base_result,
                                'increase_datetime': future_row['TestDateTime'],
                                'increase_result': future_row['TestResult'],
                                'difference': future_row['TestResult'] - base_result
                            })
                if labmin_48_hours_flag:
                    return_dict['Labmin 48 hours'] = {
                                'aki' : int(labmin_48_hours_flag),
                                'atn': 0}
                    return return_dict

                # Case 2 - A
                # Check if the baseline serum creatinine exists in the dataframe
                if baseline_serum_creatinine_df.shape[0]>0:
                    
                    #Fetch the baseline serum creatinine value
                    baseline_serum_creatinine = float(baseline_serum_creatinine_df['TestResult'].max())
                    
                    #Check if the any value is greater than 1.%*(baseline serum creatinine)
                    baseline_serum_creatinine_flag = creatinine_df['TestResult'].apply(
                                                            lambda x: 1 if float(x) >= baseline_serum_creatinine*1.5 else 0).max()

                    #Check if the serum creatinine flag is being flagged by baseline serum creatinine.
                    return_dict['Baseline Serum Creatinine'] = {
                                'aki' : int(baseline_serum_creatinine_flag),
                                'atn': 0}
                    return return_dict    

                # Case 2 - B
                # Check if the serum creatinine is being flagged because of universal lab min.
                labmin_universal_flag = creatinine_df['TestResult'].apply(
                                                lambda x: 1 if float(x) >= labmin_universal*1.5 else 0).max()
                if labmin_universal_flag:
                    return_dict['Labmin Universal'] = {
                                'aki' : int(labmin_universal_flag),
                                'atn': 0}
                    return return_dict
            else:
                pass
        else:
            return None

    def get_boolean(self):
        """ 
        function use to generate the boolean flag based on the tests result range
        """
        return_dict = {}

        if self.postprocess_data.shape[0] > 0:
            for test_name, test_range_dict in self.test_name_range_dict.items():
                test_result_list = self.postprocess_data[self.postprocess_data['TestName']
                                                         == test_name]['TestResult'].to_list()
                test_result_list_ = []
                try:
                    test_result_list_ = [float(i)
                                         for i in test_result_list if i]
                except ValueError as e:
                    pass
                if test_result_list_ and test_name not in return_dict.keys():
                    # update aki/atn flag list
                    if test_name in ['Urine Osmolality', 'Urine Specific Gravity', 'Blood Urea Nitrogen (BUN)', 'Urine Creatinine',]:
                        if max(test_result_list_) > test_range_dict.get('aki'):
                            return_dict[test_name] = {'aki': 1, 'atn': 0}

                        elif min(test_result_list_) < test_range_dict.get('atn'):
                            return_dict[test_name] = {'aki': 0, 'atn': 1}
                        else:
                            return_dict[test_name] = {'aki': 0, 'atn': 0}
                    elif test_name in ['Urine Sodium']:
                        if min(test_result_list_) < test_range_dict.get('aki'):
                            return_dict[test_name] = {'aki': 1, 'atn': 0}
                        else:
                            return_dict[test_name] = {'aki': 0, 'atn': 0}
                        if max(test_result_list_) > test_range_dict.get('atn'):
                            return_dict[test_name] = {'aki': 0, 'atn': 1}
                        else:
                            return_dict[test_name] = {'aki': 0, 'atn': 0}
                    elif test_name in ['Bilirubin', 'Potassium', 'Sodium', "Urine Protein", "Urine Volume (e.g. I&O's)", 'Serum Creatinine',]:
                        # noraml range (0: Normal, 1: Abnormal) (atn is not depends on above test, so flag is always 0 )
                        flag_min = min(test_result_list_) < test_range_dict.get(
                            'normal_range')[0]
                        flag_max = max(test_result_list_) > test_range_dict.get(
                            'normal_range')[1]
                        return_dict[test_name] = {'aki': int(
                            any((flag_min, flag_max))), 'atn': 0}
                        
                    elif test_name in ['WBC', 'Bands (i.e. left-shift)', 'Fever', 'Procalcitonin', 'C-Reactive Protein (CRP)']:
                        if not (test_range_dict.get('pneumonia')[1] == None or test_range_dict.get('pneumonia')[0] == None):
                            if max(test_result_list_) > test_range_dict.get('pneumonia')[1] or min(test_result_list_) < test_range_dict.get('pneumonia')[0]:
                                return_dict[test_name] = {'pneumonia': 1}
                            else:
                                return_dict[test_name] = {'pneumonia': 0}
                        else:
                            if test_range_dict.get('pneumonia')[1]:
                                if max(test_result_list_) > test_range_dict.get('pneumonia')[1]:
                                    return_dict[test_name] = {'pneumonia': 1}
                                else:
                                    return_dict[test_name] = {'pneumonia': 0}
                            elif test_range_dict.get('pneumonia')[0]:
                                if min(test_result_list_) < test_range_dict.get('pneumonia')[0]:
                                    return_dict[test_name] = {'pneumonia': 1}
                                else:
                                    return_dict[test_name] = {'pneumonia': 0}
                    elif test_name in ['PT', 'INR', 'APTT', 'Platelets',]:
                        if not (test_range_dict.get('sepsis')[1] == None or test_range_dict.get('sepsis')[0] == None):
                            if max(test_result_list_) > test_range_dict.get('sepsis')[1] or min(test_result_list_) < test_range_dict.get('sepsis')[0]:
                                return_dict[test_name] = {'sepsis': 1}
                            else:
                                return_dict[test_name] = {'sepsis': 0}
                        else:
                            if test_range_dict.get('sepsis')[1]:
                                if max(test_result_list_) > test_range_dict.get('sepsis')[1]:
                                    return_dict[test_name] = {'sepsis': 1}
                                else:
                                    return_dict[test_name] = {'sepsis': 0}
                            elif test_range_dict.get('sepsis')[0]:
                                if min(test_result_list_) < test_range_dict.get('sepsis')[0]:
                                    return_dict[test_name] = {'sepsis': 1}
                                else:
                                    return_dict[test_name] = {'sepsis': 0}

                    elif test_name in   ["Thiamine Test","Vitamin B12","Blood Urea Nitrogen (BUN)","Ammonia Level Test","GFR Test","Urine Creatinine","Urine Osmolality","Sodium","Potassium","Thyroid Stimulating Hormone (TSH)","Free Thyroxine (FT4)","Thyroxine (T4)","Triiodothyronine (FT3)","Serum Cortisol","WBC","Bands (i.e. left-shift)","RBC","Hemoglobin Test","Relative Lymphocytes","Absolute Lymphocytes","Hematocrit Test","PT","Bilirubin","Direct Bilirubin","Alanine Transaminase (ALT)","Aspartate Transaminase (AST)","Alkaline Phosphatase","Albumin","Total Protein","Gamma-glutamyltransferase (GGT)","Lactate Dehydrogenase (LDH)","Chloride","Uric Acid, Serum","PaO2, Arterial","PaCO2","HCO3","Alcohol Level","Acetaminophen Test","Glucose","Calcium","Magnesium","Phosphate","Carbon Dioxide","INR","PTT","Serum Creatinine"]:
                        if not (test_range_dict.get('encephalopathy')[1] == None or test_range_dict.get('encephalopathy')[0] == None):
                            if max(test_result_list_) > test_range_dict.get('encephalopathy')[1] or min(test_result_list_) < test_range_dict.get('encephalopathy')[0]:
                                return_dict[test_name] = {'encephalopathy': 1}
                            else:
                                return_dict[test_name] = {'encephalopathy': 0}
                        else:
                            if test_range_dict.get('encephalopathy')[1]:
                                if max(test_result_list_) > test_range_dict.get('encephalopathy')[1]:
                                    return_dict[test_name] = {'encephalopathy': 1}
                                else:
                                    return_dict[test_name] = {'encephalopathy': 0}
                            elif test_range_dict.get('encephalopathy')[0]:
                                if min(test_result_list_) < test_range_dict.get('encephalopathy')[0]:
                                    return_dict[test_name] = {'encephalopathy': 1}
                                else:
                                    return_dict[test_name] = {'encephalopathy': 0}
                    # elif test_name in ['Cardiac troponin', 'Creatine kinase']:
                    #     if not (test_range_dict.get('ami')[1] == None or test_range_dict.get('ami')[0] == None):
                    #         if max(test_result_list_) > test_range_dict.get('ami')[1] or min(test_result_list_) < test_range_dict.get('ami')[0]:
                    #             return_dict[test_name] = {'ami': 1}
                    #         else:
                    #             return_dict[test_name] = {'ami': 0}
                    #     else:
                    #         if test_range_dict.get('ami')[1]:
                    #             if max(test_result_list_) > test_range_dict.get('ami')[1]:
                    #                 return_dict[test_name] = {'ami': 1}
                    #             else:
                    #                 return_dict[test_name] = {'ami': 0}
                    #         elif test_range_dict.get('ami')[0]:
                    #             if min(test_result_list_) < test_range_dict.get('ami')[0]:
                    #                 return_dict[test_name] = {'ami': 1}
                    #             else:
                    #                 return_dict[test_name] = {'ami': 0}
                    elif test_name in ["Glassgow Coma Scale",'Thiamine Test','Vitamin B12','Ammonia Level','GFR','Serum Cortisol','RBC','Hemoglobin','Hematocrit','Direct Bilirubin','Alanine Transaminase (ALT)','Aspartate Transaminase (AST)','Alkaline Phosphatase','Albumin','Total Protein','Gamma-glutamyltransferase (GGT)','Chloride','Blood Alcohol Level','Acetaminophen Level',"Sodium", "Potassium"]:
                        if not (test_range_dict.get('encephalopathy')[1] == None or test_range_dict.get('encephalopathy')[0]==None):
                            if max(test_result_list_) > test_range_dict.get('encephalopathy')[1] or min(test_result_list_) < test_range_dict.get('encephalopathy')[0]:
                                return_dict[test_name] = {'encephalopathy': 1}
                            else:
                                return_dict[test_name] = {'encephalopathy': 0}
                        else:
                            if test_range_dict.get('encephalopathy')[1]:
                                if max(test_result_list_) > test_range_dict.get('encephalopathy')[1]:
                                    return_dict[test_name] = {'encephalopathy': 1}
                                else:
                                    return_dict[test_name] = {'encephalopathy': 0}
                            elif test_range_dict.get('encephalopathy')[0]:
                                if min(test_result_list_) < test_range_dict.get('encephalopathy')[0]:
                                    return_dict[test_name] = {'encephalopathy': 1}
                                else:
                                    return_dict[test_name] = {'encephalopathy': 0}

            abnormal_vital_ls = self.postprocess_data[(self.postprocess_data['IsAbnormal'].isin([1.0, 1])) & (
                self.postprocess_data['TestName'].isin(self.vital_test_list))]['TestName'].to_list()
            abnormal_vital_ls_ = list(set(abnormal_vital_ls))
            for vital_ in abnormal_vital_ls_:
                return_dict[vital_] = {'vital': 1}
        
        else:
            print('no postprocess_data found')

        creat_flag_dict = self.get_creatine_boolean()
        return_dict.update(creat_flag_dict) if creat_flag_dict else return_dict

        if self.post_process_excerpt_df.shape[0] > 0:
            # first_row_of_each_unique = self.post_process_excerpt_df.groupby(
            #     'TestName').first().reset_index()
            # test_list_with_abnormal_flag = first_row_of_each_unique[first_row_of_each_unique['IsAbnormal']
            #                                                         == 1]['TestName'].to_list()
            # for test_excp in test_list_with_abnormal_flag:
            #     if test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
            #             'aki').get('attribute_dict').keys():
            #         return_dict[test_excp] = {'aki': 1, 'atn': 1}
            #     elif test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
            #             'pneumonia').get('attribute_dict').keys():
            #         return_dict[test_excp] = {'pneumonia': 1}
            #     elif test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
            #             'ami').get('attribute_dict').keys():
            #         return_dict[test_excp] = {'ami': 1}

            grp = self.post_process_excerpt_df.groupby(
                'TestName')['IsAbnormal'].max()

            for test_excp, flag in grp.to_dict().items():
                if test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
                        'aki').get('attribute_dict').keys() and flag==1:
                    return_dict[test_excp] = {'aki': 1, 'atn': 1}
                elif test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
                        'pneumonia').get('attribute_dict').keys() and flag == 1:
                    return_dict[test_excp] = {'pneumonia': 1}
                elif test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
                        'sepsis').get('attribute_dict').keys() and flag == 1:
                    return_dict[test_excp] = {'sepsis': 1}
                elif test_excp not in return_dict and test_excp in self.all_attribute_map_dict.get(
                        'ami').get('attribute_dict').keys() and flag == 1:
                    return_dict[test_excp] = {'ami': 1}

        return return_dict

    def get_boolean_json_data(self, s3_c, bucket_name, postprocess_result_path, save_path):
        flag_dict = self.get_boolean()
        flag_dict['ResultLink'] = postprocess_result_path

        js_path = save_path.split('/')[-1].replace('.csv', '')
        # with open(rf"{save_path}\{js_path}_analysis_result.json", 'w') as f:
        #     json.dump(flag_dict, f, indent=4)
        put_resp_flag = s3_c.put_object(Bucket=bucket_name, Body=json.dumps(flag_dict),
                                        Key=rf'{save_path}/{js_path}_analysis_result.json')
        return flag_dict