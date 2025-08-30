import json


class SepsisAnalysis:
    def __init__(self, postprocess_data,post_process_excerpt_df, test_name_range_dict,vital_regex_list):
        self.postprocess_data = postprocess_data
        self.post_process_excerpt_df = post_process_excerpt_df
        self.test_name_range_dict = test_name_range_dict
        self.sepsis_lab_list = test_name_range_dict.keys()
        self.vital_test_list = vital_regex_list

    @staticmethod
    def get_ref_range_and_boolean_flag(test_name, test_result, test_name_range_dict):
        if test_result: 
            test_result = float(test_result)
            if not (test_name_range_dict.get(test_name).get('sepsis')[1] == None or test_name_range_dict.get(test_name).get('sepsis')[0] == None):
                if test_result > test_name_range_dict.get(test_name).get('sepsis')[1] or test_result < test_name_range_dict.get(test_name).get('sepsis')[0]:
                    return test_name_range_dict.get(test_name).get('sepsis'), 1
                else:
                    return test_name_range_dict.get(test_name).get('sepsis'), 0
            else:
                if test_name_range_dict.get(test_name).get('sepsis')[1]:
                    if test_result > test_name_range_dict.get(test_name).get('sepsis')[1]:
                        return test_name_range_dict.get(test_name).get('sepsis'), 1
                    else:
                        return test_name_range_dict.get(test_name).get('sepsis'), 0
                elif test_name_range_dict.get(test_name).get('sepsis')[0]:
                    if test_result < test_name_range_dict.get(test_name).get('sepsis')[0]:
                        return test_name_range_dict.get(test_name).get('sepsis'), 1
                    else:
                        return test_name_range_dict.get(test_name).get('sepsis'), 0
        else:
            return test_name_range_dict.get(test_name).get('sepsis'), 0
    def get_boolean(self):
        """ 
        function use to generate the boolean flag based on the tests result range
        """
        return_dict = {}
        # postprocess_data = pd.read_csv(self.post_process_csv)

        if self.postprocess_data.shape[0] > 0:
            for test_name, test_range_dict in self.test_name_range_dict.items():
                test_result_list = self.postprocess_data[self.postprocess_data['TestName']
                                                    == test_name]['TestResult'].to_list()
                test_result_list_ = []
                try:
                    test_result_list_ = [float(i) for i in test_result_list if i]
                except ValueError as e:
                    pass
                if test_result_list_ and test_name not in return_dict.keys():
                    # update sepsis flag list
                    if test_name in self.sepsis_lab_list:
                        if not (test_range_dict.get('sepsis')[1] == None or test_range_dict.get('sepsis')[0]==None):
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
            
            abnormal_vital_ls = self.postprocess_data[(self.postprocess_data['IsAbnormal'].isin([1.0, 1])) & (
                self.postprocess_data['TestName'].isin(self.vital_test_list))]['TestName'].to_list()
            abnormal_vital_ls_ = list(set(abnormal_vital_ls))
            for vital_ in abnormal_vital_ls_:
                return_dict[vital_] = {'vital': 1}
                    
        else:
            print('no postprocess_data found')
        if self.post_process_excerpt_df.shape[0]>0:
            # first_row_of_each_unique = self.post_process_excerpt_df.groupby(
            #     'TestName').first().reset_index()
            # test_list_with_abnormal_flag = first_row_of_each_unique[first_row_of_each_unique['IsAbnormal']
            #                                                         == 1]['TestName'].to_list()
            # for test_excp in test_list_with_abnormal_flag:
            #     if test_excp not in return_dict:
            #         return_dict[test_excp] = {'pneumonia': 1}
            
            grp = self.post_process_excerpt_df.groupby(
                'TestName')['IsAbnormal'].max()
            
            for test_excp, flag in grp.to_dict().items():
                if test_excp not in return_dict and flag==1:
                    return_dict[test_excp] = {'sepsis': 1}
                
        return return_dict

    def get_boolean_json_data(self,s3_c, bucket_name, postprocess_result_path, save_path):
        flag_dict = self.get_boolean()
        flag_dict['ResultLink'] = postprocess_result_path

        js_path = save_path.split('/')[-1].replace('.csv', '')
        # with open(rf"{save_path}\{js_path}_analysis_result.json", 'w') as f:
        #     json.dump(flag_dict, f, indent=4)
        s3_c.put_object(Bucket=bucket_name, Body=json.dumps(flag_dict),
                                        Key=rf'{save_path}/{js_path}_analysis_result.json')
        return flag_dict
