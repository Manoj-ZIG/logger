from rapidfuzz import fuzz, process
import pandas as pd
import logging
import warnings
warnings.filterwarnings("ignore")
try:
    from postprocess.post_process import Postprocess
    from analysis.aki_analytics import AKIAnalysis
    from analysis.vital_analytics import VitalAnalytics
    from utility.read_csv_from_s3 import put_object_to_s3
    from utility.get_value_suppression import suppress_value
    from utility.suppress_test_over_table_region import align_bb, suppress_test_over_tableRegion
    from utility.fuzzymatcher import FuzzMatcher
except ModuleNotFoundError as e:
    from .post_process import Postprocess
    from ..analysis.aki_analytics import AKIAnalysis
    from ..analysis.vital_analytics import VitalAnalytics
    from ..utility.read_csv_from_s3 import put_object_to_s3
    from ..utility.get_value_suppression import suppress_value
    from ..utility.suppress_test_over_table_region import align_bb, suppress_test_over_tableRegion
    from ..utility.fuzzymatcher import FuzzMatcher

class GetPostProcessData:
    def __init__(self, test_name_range_dict, vital_ref_range_dict,component_dict):
        self.test_name_range_dict = test_name_range_dict
        self.vital_ref_range_dict = vital_ref_range_dict
        self.inclusion_dict=component_dict['inclusion_dict']
        self.exclusion_dict=component_dict['exclusion_dict']
        self.attribute_thresholds=component_dict['attribute_thresholds']

    def get_model_data(self, data):
        doc_name, page_no, doc_link, raw_test_name,raw_test_result,raw_test_date,test_name, test_result, test_unit, ref_range, \
            date, is_abnormal, date_from, section, sub_section, main_section, is_from, bb_info = data
        model_data_dict = {'DocumentName': doc_name.replace(".csv", ""),
                           'Page': page_no,
                           'DocumentLink': '',
                           'RawTestName':raw_test_name,
                           'RawTestResult':raw_test_result,
                           'RawTestDateTime':raw_test_date,  
                           'TestName': test_name,
                           'TestResult': test_result,
                           'TestUnit': test_unit,
                           'ReferenceRange': ref_range,
                           'TestDateTime': date,
                           'IsAbnormal': is_abnormal,
                           'DateFrom': date_from,
                           'Section': section,
                           'SubSection': sub_section,
                           'MainSection': main_section,
                           'IsFrom': is_from,
                           'BBInfo': bb_info
                           }
        return model_data_dict

    def suppress_sbp_dbp(self, df):
        df1_sbp_dbp = df[df['TestName'].isin(['Blood Pressure - Diastolic',
                                              'Blood Pressure - Systolic'])]
        if df1_sbp_dbp.shape[0] > 0:
            df1_sbp_dbp2 = df1_sbp_dbp.drop_duplicates(
                subset=['TestName', 'TestResult', 'TestDateTime'])

            b = df1_sbp_dbp2.groupby(['TestDateTime', 'TestName']).agg(
                u_test_res=('TestResult', 'nunique')).unstack().reset_index()

            b.columns = ['TestDateTime', 'Blood Pressure - Diastolic',
                         'Blood Pressure - Systolic']
            b.dropna(inplace=True, ignore_index=True)

            # date_time to be consider (other than below date)
            date_list = list(b[b['Blood Pressure - Diastolic'] ==
                               b['Blood Pressure - Systolic']]['TestDateTime'].unique())
            sbp_dbp_df = df1_sbp_dbp2[df1_sbp_dbp2['TestDateTime'].isin(
                date_list)]
            return sbp_dbp_df
        else:
            return pd.DataFrame({})
        
    def suppress_ed_notes(self, df):
        df_ed = df[df['MainSection'] == 'Emergency provider report']
        if df_ed.shape[0] > 0:
            for i, row in enumerate(df_ed.iterrows()):
                condition = (df['TestName'] == df_ed.iloc[i]['TestName']) & (df['TestResult'] == df_ed.iloc[i]['TestResult'])
                if df[condition].shape[0] > 1:
                    condition2 = condition & (df['MainSection'] == df_ed.iloc[i]['MainSection'])
                    df.drop(df[condition2].index, inplace = True)
        return df

    def get_postprocess_data(self, pre_df, p_obj, default_year):
        """ 
        function use to postprocess the extracted data
        args:
            pre_df: pandas df (raw data dataframe)
            p_obj: Postprocess object
            default_year: str 
        return:
            post_process_data: list
        """
        post_process_data = []
        fuzzObj = FuzzMatcher(60,self.inclusion_dict,self.exclusion_dict)

        if pre_df.shape[0] > 0:
            for i, row in pre_df.iterrows():
                try:
                    doc_name, page_no, test_name, test_result, date, test_unit, section, sub_section, main_section, date_from, is_from, bb_info = str(row['DocumentName']), \
                        int(row['Page']), str(row['TestName']), str(row['TestResult']), str(
                            row['TestDateTime']), str(row['TestUnit']), row['Section'], row['SubSection'], row['MainSection'], row['DateFrom'], row['IsFrom'], row['BBInfo']
                except KeyError as e:
                    doc_name, page_no, test_name, test_result, date,  section, sub_section, main_section, date_from, is_from, bb_info = str(row['DocumentName']), \
                        int(row['Page']), str(row['TestName']), str(row['TestResult']), str(
                            row['TestDateTime']), row['Section'], row['SubSection'],  row['MainSection'], row['DateFrom'], row['IsFrom'], row['BBInfo']

                post_process_test_result = None
                post_process_test_result_ = None
                test_name_ = None
                test_name__ = None
                ref_range, ref_range_ = None, None
                abnormal_flag, abnormal_flag_ = None, None
                raw_test_name = test_name
                raw_test_result=test_result
                raw_test_date=date                 
                attr,score=fuzzObj.get_match_component_v2(test_name)

                
                if attr == 'Sodium' and  score>=self.attribute_thresholds['Sodium']:
                    post_process_test_result, test_name_unit = Postprocess.sodium_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Potassium'and score>=self.attribute_thresholds['Potassium']:
                    post_process_test_result, test_name_unit = Postprocess.potassium_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Serum Creatinine' and score>=self.attribute_thresholds['Serum Creatinine']:
                    post_process_test_result, test_name_unit = Postprocess.creatinine_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Blood Urea Nitrogen (BUN)' and score>=self.attribute_thresholds['Blood Urea Nitrogen (BUN)']:
                    post_process_test_result, test_name_unit = Postprocess.bun_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Bilirubin' and score>=self.attribute_thresholds['Bilirubin']:
                    post_process_test_result, test_name_unit = Postprocess.bilirubin_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Urine Specific Gravity' and score>=self.attribute_thresholds['Urine Specific Gravity']:
                    post_process_test_result, test_name_unit = Postprocess.urine_specific_gravity_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Urine Osmolality' and score>=self.attribute_thresholds['Urine Osmolality']:
                    post_process_test_result, test_name_unit = Postprocess.urine_osmolality_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                   
                elif attr == 'Urine Creatinine' and score>=self.attribute_thresholds['Urine Creatinine']:
                    post_process_test_result, test_name_unit = Postprocess.urine_creatinine_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Urine Sodium' and score>=self.attribute_thresholds['Urine Sodium']:
                    post_process_test_result, test_name_unit = Postprocess.sodium_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Urine Protein' and score>=self.attribute_thresholds['Urine Protein']:
                    post_process_test_result, test_name_unit = Postprocess.urine_protein_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
            
                elif attr == 'Pulse' and score>=self.attribute_thresholds['Pulse']:
                    post_process_test_result = Postprocess.pulse_rate_postprocess(
                        test_result)
                    test_name_ = attr
                    test_name_unit = 'bpm'
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    
                elif attr == 'Blood Pressure' and score>=self.attribute_thresholds['Blood Pressure']:
                    sbp_match = process.extractOne(test_name.strip().lower(
                    ), ['systolic blood pressure', 'sbp', 'nibp systolic'], scorer=fuzz.token_sort_ratio)
                    dbp_match = process.extractOne(test_name.strip().lower(
                    ), ['diastolic blood pressure', 'dbp','nibp diastolic'], scorer=fuzz.token_sort_ratio)
                    # if test_name.strip().lower() in ['systolic blood pressure']:
                    if sbp_match[1] >= 64 and '/' not in str(test_result):
                        post_process_test_result,_ = Postprocess.blood_pressure_postprocess(
                            test_result)
                        test_name_ = 'Blood Pressure - Systolic'
                        ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                            test_name_, post_process_test_result, self.vital_ref_range_dict)
                        test_name_unit = 'mm Hg'
                    elif dbp_match[1] >= 64 and '/' not in str(test_result):
                        post_process_test_result,_ = Postprocess.blood_pressure_postprocess(
                            test_result)
                        test_name_ = 'Blood Pressure - Diastolic'
                        ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                            test_name_, post_process_test_result, self.vital_ref_range_dict)
                        test_name_unit = 'mm Hg'
                    elif '/' in str(test_result):

                        post_process_test_result, post_process_test_result_ = Postprocess.blood_pressure_postprocess(
                            test_result)
                        if post_process_test_result and post_process_test_result_:
                            test_name_, test_name__ = 'Blood Pressure - Systolic', 'Blood Pressure - Diastolic'
                            ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                                test_name_, post_process_test_result, self.vital_ref_range_dict)
                            ref_range_, abnormal_flag_ = VitalAnalytics.get_ref_range_and_boolean_flag(
                                test_name__, post_process_test_result_, self.vital_ref_range_dict)
                            test_name_unit = 'mm Hg'

                    
                elif attr == 'SpO2' and score>=self.attribute_thresholds['SpO2']:
                    post_process_test_result = Postprocess.spo2_postprocess(
                        test_result)
                    test_name_ = attr
                    test_name_unit = '%'
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    
                elif attr == 'Temperature' and score>=self.attribute_thresholds['Temperature']:
                    post_process_test_result = Postprocess.temperature_postprocess(
                        test_result)
                    test_name_ = attr
                    test_name_unit = 'Fahrenheit'
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    
                elif attr == 'Rate of Oxygen' and score>=self.attribute_thresholds['Rate of Oxygen']:
                    post_process_test_result = Postprocess.o2_flow_rate_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    test_name_unit = 'lpm'
                elif attr == 'Respiration' and score>=self.attribute_thresholds['Respiration']:
                    post_process_test_result = Postprocess.respiratory_rate_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    test_name_unit = 'br/min'

                else:
                    pass

                post_process_date = p_obj.date_parser(
                    str(date), (default_year, 1, 1))

                if post_process_test_result_ and test_name_ and post_process_date:
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '',raw_test_name,raw_test_result,raw_test_date,test_name_, post_process_test_result,
                            test_name_unit, ref_range, post_process_date, abnormal_flag, date_from, section, sub_section, main_section, is_from, bb_info)))
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '',raw_test_name,raw_test_result,raw_test_date,test_name__, post_process_test_result_,
                            test_name_unit, ref_range_, post_process_date, abnormal_flag_, date_from, section, sub_section, main_section, is_from, bb_info)))
                elif post_process_test_result and test_name_ and post_process_date:
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '',raw_test_name,raw_test_result,raw_test_date,test_name_, post_process_test_result,
                            test_name_unit, ref_range, post_process_date, abnormal_flag, date_from, section, sub_section, main_section, is_from, bb_info)))
                else:
                    pass
        return post_process_data

    def get_postprocess_data_for_excerpt(self, pre_df, p_obj, default_year, attribute_ls):
        """
        function use to postprocess the extracted data
        args:
            pre_df: pandas df (raw data dataframe)
            p_obj: Postprocess object
            default_year: str
        return:
            post_process_data: list
        """

        pre_df['IsAbnormal'] = ''
        pre_df['ReferenceRange'] = ''
        pre_df['TestUnit'] = ''
        pre_df['DocumentLink'] = ''
        pre_df['DateFrom'] = [0]*pre_df.shape[0]
        pre_df['IsFrom'] = 'excerpt'
        pre_df['RawTestName'] = ''
        pre_df['RawTestResult'] = '' 
        pre_df['RawTestDateTime'] = '' 
                 
        for i, row in pre_df.iterrows():

            post_process_test_result = None
            # post_process_test_result_ = None
            # test_name_ = None
            post_process_date = p_obj.date_parser(
                str(row['TestDateTime']), (default_year, 1, 1))
            bb_info = row['SpecificTermBB']
            if row['TestName'] == "Urine Volume (e.g. I&O\'s)":
                post_process_test_result, test_unit = Postprocess.urine_volume_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info
            
            elif row['TestName'] == "Serum Creatinine":
                post_process_test_result, test_unit = Postprocess.creatinine_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info

            elif row['TestName'] == "Urine Creatinine":
                post_process_test_result, test_unit = Postprocess.urine_creatinine_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info
            
            elif row['TestName'] == "Blood Urea Nitrogen (BUN)":
                post_process_test_result, test_unit = Postprocess.bun_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info

            elif row['TestName'] == "Urine Osmolality":
                post_process_test_result, test_unit = Postprocess.urine_osmolality_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info

            elif row['TestName'] == "Urine Specific Gravity":
                post_process_test_result, test_unit = Postprocess.urine_specific_gravity_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info

            elif row['TestName'] == "Sodium":
                post_process_test_result, test_unit = Postprocess.sodium_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info

            elif row['TestName'] == "Potassium":
                post_process_test_result, test_unit = Postprocess.potassium_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                
            elif row['TestName'] in attribute_ls:
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = 0 if row['isNegated'] else 1
                pre_df.at[i, 'ReferenceRange'] = ''
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = ''
                pre_df.at[i, 'BBInfo'] = bb_info
            else:
                pass
        numeric_df = pre_df[pre_df['TestName'].isin(
            ["Urine Volume (e.g. I&O\'s)", 'Serum Creatinine', 'Urine Creatinine',
             'Blood Urea Nitrogen (BUN)', 'Urine Osmolality', 'Urine Specific Gravity', 'Sodium', 'Potassium'])]
        numeric_df_ = pd.DataFrame()
        pre_df['TestDateTime'] = pre_df['TestDateTime'].apply(lambda x: p_obj.date_parser(
            str(x), (default_year, 1, 1))).to_list()
        pre_df['TestDateTime'] = pre_df['TestDateTime'].apply(
            lambda x: x if x else '')
        if numeric_df.shape[0] > 0:
            numeric_df_ = numeric_df[['DocumentName', 'Page', 'DocumentLink','RawTestName','RawTestResult','RawTestDateTime','TestName', 'TestResult', 'TestUnit', 'ReferenceRange', 'TestDateTime', 'IsAbnormal', 'Section', 'SubSection', 'MainSection',
                                      'DateFrom', 'IsFrom', 'BBInfo']]
        return pre_df, numeric_df_

    def get_postprocess_df(self, s3_c, bucket_name, save_path, post_process_obj, default_year, raw_data_df, raw_data_excerpt_df, attribute_ls, path_to_save_logs):
        """" 
        function use to post process the data from raw data that being extracted.
        args:
            save_path: str
            post_process_obj: Postprocess object
            default_year: str
            raw_data_df: pandas dataframe (extracted result dataframe)
        return:
            postprocess_df,None: pandas dataframe or None
        """
        postprocess_df, postprocess_df_output_file, post_process_excerpt_df, post_process_excerpt_df_output_file = pd.DataFrame(),None, pd.DataFrame(), None
        if raw_data_df.shape[0] > 0 or raw_data_excerpt_df.shape[0] > 0:
            post_process_data = self.get_postprocess_data(
                raw_data_df, post_process_obj, default_year)
            post_process_excerpt_df, post_process_num_excerpt_df = self.get_postprocess_data_for_excerpt(
                raw_data_excerpt_df, post_process_obj,
                default_year, attribute_ls
            )
            if not post_process_num_excerpt_df.empty:
                post_process_num_excerpt_df['BBInfo'] = post_process_num_excerpt_df['BBInfo'].apply(lambda x: align_bb(eval(x)[0][0]) if eval(x)[0] else (None, None, None, None))
            postprocess_df = pd.DataFrame(post_process_data)    
            if len(post_process_data) > 0:
                
                table_entity_file_name = rf"{save_path.split('/')[-1]}_postprocessTableEntity.csv"
                table_entity_suppress_file_name = rf"{save_path.split('/')[-1]}_postprocessSuppressTableEntity.csv"
                postprocess_df = pd.concat(
                    [postprocess_df, post_process_num_excerpt_df], ignore_index=True)
                sbp_dbp_df = self.suppress_sbp_dbp(postprocess_df)
                dff_ = postprocess_df[~postprocess_df['TestName'].isin(['Blood Pressure - Diastolic',
                                                                        'Blood Pressure - Systolic'])]
                postprocess_df_ = pd.concat(
                    [dff_, sbp_dbp_df], ignore_index=True)
                postprocess_df = suppress_value(postprocess_df_)

                postprocess_df, excp_intersect_df = suppress_test_over_tableRegion(postprocess_df.copy())

                postprocess_df = self.suppress_ed_notes(postprocess_df)
                postprocess_df.index = pd.RangeIndex(postprocess_df.shape[0])

                postprocess_df['DocumentName'] = [
                    f"{save_path.split('/')[-1]}"]*postprocess_df.shape[0]
                postprocess_df['DocumentLink'] = postprocess_df.apply(
                    lambda x: f"{x['DocumentName']}_highlighted.pdf#page={x['Page']}", axis=1)

                put_object_to_s3(s3_c, bucket_name, save_path,
                                 table_entity_file_name, postprocess_df)
                
                put_object_to_s3(s3_c, bucket_name, path_to_save_logs,
                                 table_entity_suppress_file_name, excp_intersect_df)  

                # postprocess_df.to_csv(
                #     postprocess_csv_path, index=False)
                postprocess_df_output_file = f"{save_path}/{table_entity_file_name}"
                logging.info(
                    'concatenate table df and numeric df from excerpts ...')
            else:
                logging.info('No postprocess data generated for table ...')
            if post_process_excerpt_df.shape[0] > 0:
                excerpt_entity_file_name = rf"{save_path.split('/')[-1]}_postprocessExcerptEntity.csv"

                post_process_excerpt_df['DocumentName'] = [
                    f"{save_path.split('/')[-1]}"]*post_process_excerpt_df.shape[0]
                post_process_excerpt_df['DocumentLink'] = post_process_excerpt_df.apply(
                    lambda x: f"{x['DocumentName']}_highlighted.pdf#page={x['Page']}", axis=1)
                put_object_to_s3(s3_c, bucket_name, save_path,
                                 excerpt_entity_file_name, post_process_excerpt_df)
                post_process_excerpt_df_output_file = f"{save_path}/{excerpt_entity_file_name}"

            else:
                logging.info('No postprocess data generated for excerpt ...')

        else:
            logging.info('No raw_data_df found...')
        return postprocess_df, postprocess_df_output_file, post_process_excerpt_df, post_process_excerpt_df_output_file
