from rapidfuzz import fuzz, process
import pandas as pd
import logging
import warnings
warnings.filterwarnings("ignore")

try:
    from postprocess.post_process import Postprocess
    from analysis.pneumonia_analytics import PneumoniaAnalysis
    from analysis.aki_analytics import AKIAnalysis
    from analysis.vital_analytics import VitalAnalytics
    from analysis.ami_analytics import AMIAnalysis
    from analysis.sepsis_analytics import SepsisAnalysis
    from analysis.encephalopathy_analytics import EncephalopathyAnalysis
    from utility.read_csv_from_s3 import put_object_to_s3
    from utility.get_value_suppression import suppress_value
    from utility.suppress_test_over_table_region import align_bb, suppress_test_over_tableRegion
    from utility.fuzzymatcher import FuzzMatcher
except ModuleNotFoundError as e:
    from .post_process import Postprocess
    from ..analysis.pneumonia_analytics import PneumoniaAnalysis
    from ..analysis.aki_analytics import AKIAnalysis
    from ..analysis.vital_analytics import VitalAnalytics
    from ..analysis.ami_analytics import AMIAnalysis
    from ..analysis.sepsis_analytics import SepsisAnalysis
    from ..analysis.encephalopathy_analytics import EncephalopathyAnalysis
    from ..utility.read_csv_from_s3 import put_object_to_s3
    from ..utility.get_value_suppression import suppress_value
    from ..utility.suppress_test_over_table_region import align_bb, suppress_test_over_tableRegion
    from ..utility.fuzzymatcher import FuzzMatcher


class GetPostProcessData:
    def __init__(self, test_name_range_dict, vital_ref_range_dict,zai_lab_incl_excl_threshold_params):
        self.test_name_range_dict = test_name_range_dict
        self.vital_ref_range_dict = vital_ref_range_dict
        self.inclusion_dict=zai_lab_incl_excl_threshold_params['inclusion_dict']
        self.exclusion_dict=zai_lab_incl_excl_threshold_params['exclusion_dict']
        self.attribute_thresholds=zai_lab_incl_excl_threshold_params['attribute_thresholds']

    def get_model_data(self, data):
        doc_name, page_no, doc_link,raw_test_name,raw_test_result,raw_test_date,test_name, test_result, test_unit, ref_range, \
            date, is_abnormal, date_from, section, sub_section, main_section, is_from, bb_info, is_drop = data
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
                           'BBInfo': bb_info,
                           'IsDrop': is_drop
                           }
        return model_data_dict

    def suppress_sbp_dbp(self, df):
        logging.info("FUNCTION START")
        df1_sbp_dbp = df[df['TestName'].isin(['Blood Pressure - Diastolic',
                                            'Blood Pressure - Systolic'])]
        logging.info(f'sbp_dbp shape {df1_sbp_dbp.shape}')
        if df1_sbp_dbp.shape[0] > 0:
            df1_sbp_dbp2 = df1_sbp_dbp.drop_duplicates(
                subset=['TestName', 'TestResult', 'TestDateTime'])

            b = df1_sbp_dbp2.groupby(['TestDateTime', 'TestName']).agg(
                u_test_res=('TestResult', 'nunique')).unstack().reset_index()

            # if only one of the test present/extracted
            if len(b.columns.to_list()) == 3:
                b.columns = ['TestDateTime', 'Blood Pressure - Diastolic',
                            'Blood Pressure - Systolic']
                b.dropna(inplace=True, ignore_index=True)

                # date_time to be consider (other than below date)
                date_list = list(b[b['Blood Pressure - Diastolic'] ==
                                b['Blood Pressure - Systolic']]['TestDateTime'].unique())
                sbp_dbp_df = df1_sbp_dbp2[df1_sbp_dbp2['TestDateTime'].isin(
                    date_list)]
                df1_sbp_dbp2.loc[df1_sbp_dbp2[(df1_sbp_dbp2['TestDateTime'].isin(date_list)) & (df1_sbp_dbp2['IsDrop'] == 0)].index, 'IsDrop'] = 0
                df1_sbp_dbp2.loc[df1_sbp_dbp2[(~df1_sbp_dbp2['TestDateTime'].isin(date_list)) & (df1_sbp_dbp2['IsDrop'] == 0)].index, 'IsDrop'] = 1
                logging.info("FUNCTION END")
                return df1_sbp_dbp2
            else:
                # return pd.DataFrame({})
                df1_sbp_dbp2['IsDrop'] = 1
                logging.info("FUNCTION END")
                return df1_sbp_dbp2
        else:
            logging.info("FUNCTION END")
            return pd.DataFrame({})
        
    def suppress_ed_notes(self, df):
        logging.info("FUNCTION START")
        df_ed = df[df['MainSection'] == 'Emergency provider report']
        if df_ed.shape[0] > 0:
            for i, row in enumerate(df_ed.iterrows()):
                condition = (df['TestName'] == df_ed.iloc[i]['TestName']) & (df['TestResult'] == df_ed.iloc[i]['TestResult'])
                section_list = df.loc[df[condition].index,'MainSection'].to_list()
                if df[condition].shape[0] > 1 and not all(sec == "Emergency provider report" for sec in section_list):
                    condition2 = condition & (df['MainSection'] == df_ed.iloc[i]['MainSection'])
                    # df.drop(df[condition2].index, inplace = True)
                    df.loc[df[condition2].index,'IsDrop'] = 1
        logging.info("FUNCTION END")
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
                    doc_name, page_no, test_name, test_result, date, test_unit, section, sub_section, main_section, date_from, is_from, bb_info, is_drop = str(row['DocumentName']), \
                        int(row['Page']), str(row['TestName']), str(row['TestResult']), str(
                            row['TestDateTime']), str(row['TestUnit']), row['Section'], row['SubSection'], row['MainSection'], row['DateFrom'], row['IsFrom'], row['BBInfo'], row['IsDrop']
                except KeyError as e:
                    doc_name, page_no, test_name, test_result, date,  section, sub_section, main_section, date_from, is_from, bb_info, is_drop = str(row['DocumentName']), \
                        int(row['Page']), str(row['TestName']), str(row['TestResult']), str(
                            row['TestDateTime']), row['Section'], row['SubSection'],  row['MainSection'], row['DateFrom'], row['IsFrom'], row['BBInfo'], row['IsDrop']

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
                    
            
                elif attr == 'Cardiac Troponin' and score>=self.attribute_thresholds['Cardiac Troponin']:
                    post_process_test_result, test_name_unit = Postprocess.trop_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = AMIAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    

                elif attr == 'WBC' and score>=self.attribute_thresholds['WBC']:
                    post_process_test_result, test_name_unit = Postprocess.wbc_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Bands (i.e. left-shift)' and score>=self.attribute_thresholds['Bands (i.e. left-shift)']:
                    post_process_test_result, test_name_unit = Postprocess.imm_gran_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'Pulse' and score>=self.attribute_thresholds['Pulse']:
                    post_process_test_result = Postprocess.pulse_rate_postprocess(
                        test_result)
                    test_name_ = attr
                    test_name_unit = 'bpm'
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    
                elif attr == 'Blood Pressure' and score>=self.attribute_thresholds['Blood Pressure']:
                    # if test_name.strip().lower() in ['systolic blood pressure']:
                    sbp_match = process.extractOne(test_name.strip().lower(
                    ), ['systolic blood pressure', 'sbp', 'nibp systolic', 'bps mmhg', 'sys bp'], scorer=fuzz.token_sort_ratio)
                    dbp_match = process.extractOne(test_name.strip().lower(
                    ), ['diastolic blood pressure', 'dbp', 'nibp diastolic', 'bpd mmhg', 'dia bp'] , scorer=fuzz.token_sort_ratio)
                    if sbp_match[1] >= 64 and '/' not in str(test_result) and sbp_match[1] > dbp_match[1]:
                        post_process_test_result, _ = Postprocess.blood_pressure_postprocess(
                            test_result)
                        test_name_ = 'Blood Pressure - Systolic'
                        ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                            test_name_, post_process_test_result, self.vital_ref_range_dict)
                        test_name_unit = 'mm Hg'
                    elif dbp_match[1] >= 64 and '/' not in str(test_result) and dbp_match[1] > sbp_match[1]:
                        post_process_test_result, _ = Postprocess.blood_pressure_postprocess(
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
                    
                elif attr == 'Mean Arterial Pressure' and score>=self.attribute_thresholds['Mean Arterial Pressure']:
                    post_process_test_result = Postprocess.map_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.vital_ref_range_dict)
                    test_name_unit = 'mmHg'
                    
                elif attr == 'Platelets' and score>=self.attribute_thresholds['Platelets']:
                    post_process_test_result, test_name_unit = Postprocess.platelets_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'PT' and score>=self.attribute_thresholds['PT']:
                    post_process_test_result, test_name_unit = Postprocess.pt_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'INR' and score>=self.attribute_thresholds['INR']:
                    post_process_test_result, test_name_unit = Postprocess.inr_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == 'APTT' and score>=self.attribute_thresholds['APTT']:
                    post_process_test_result, test_name_unit = Postprocess.aptt_postprocess(
                        test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
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
                
                elif attr == 'FiO2' and score>= self.attribute_thresholds['FiO2']:
                    post_process_test_result = Postprocess.fio2_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                        test_name_,post_process_test_result, self.vital_ref_range_dict)
                    test_name_unit = 'mmHg'
                    
                    
                elif attr == 'Creatine Kinase' and score>=self.attribute_thresholds['Creatine Kinase']:
                    continue
                elif attr == "GFR Test" and score >= self.attribute_thresholds['GFR Test']:
                    post_process_test_result, test_name_unit =  Postprocess.gfr_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)

                elif attr == "Thiamine Test" and score >= self.attribute_thresholds['Thiamine Test']:
                    post_process_test_result, test_name_unit =  Postprocess.vitamin_B1_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Vitamin B12" and score >= self.attribute_thresholds['Vitamin B12']:
                    post_process_test_result, test_name_unit =  Postprocess.vitamin_B12_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)

                elif attr== "Ammonia Level Test" and score >= self.attribute_thresholds['Ammonia Level Test']:
                    post_process_test_result, test_name_unit =  Postprocess.ammonia_level_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)

                elif attr == "Serum Cortisol" and score >= self.attribute_thresholds['Serum Cortisol']:
                    post_process_test_result, test_name_unit =  Postprocess.serum_cortisol_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Hemoglobin Test" and score >= self.attribute_thresholds['Hemoglobin Test']:
                    post_process_test_result, test_name_unit =  Postprocess.hgb_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Hematocrit Test" and score >= self.attribute_thresholds['Hematocrit Test']:
                    post_process_test_result, test_name_unit =  Postprocess.hematocrit_test_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Direct Bilirubin" and score >= self.attribute_thresholds['Direct Bilirubin']:
                    post_process_test_result, test_name_unit =  Postprocess.direct_billirubin_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Alanine Transaminase (ALT)" and score >= self.attribute_thresholds['Alanine Transaminase (ALT)']:
                    post_process_test_result, test_name_unit =  Postprocess.alt_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Aspartate Transaminase (AST)" and score >= self.attribute_thresholds['Aspartate Transaminase (AST)']:
                    post_process_test_result, test_name_unit =  Postprocess.ast_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Alkaline Phosphatase" and score >= self.attribute_thresholds['Alkaline Phosphatase']:
                    post_process_test_result, test_name_unit =  Postprocess.alkaline_phosphate_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Gamma-glutamyltransferase (GGT)" and score >= self.attribute_thresholds['Gamma-glutamyltransferase (GGT)']:
                    post_process_test_result, test_name_unit =  Postprocess.ggt_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                    
                elif attr == "Albumin" and score >= self.attribute_thresholds['Albumin']:
                    post_process_test_result, test_name_unit =  Postprocess.albumin_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Alcohol Level" and score >= self.attribute_thresholds['Alcohol Level']:
                    post_process_test_result, test_name_unit = Postprocess.blood_alcohol_level_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Acetaminophen Test" and score >= self.attribute_thresholds['Acetaminophen Test']:
                    post_process_test_result, test_name_unit = Postprocess.acetaminophen_level_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Chloride" and score >= self.attribute_thresholds['Chloride']:
                    post_process_test_result, test_name_unit = Postprocess.chloride_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Total Protein" and score >= self.attribute_thresholds['Total Protein'] :
                    post_process_test_result, test_name_unit = Postprocess.total_protein_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "RBC" and score >= self.attribute_thresholds['RBC']:
                    post_process_test_result, test_name_unit = Postprocess.rbc_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Thyroid Stimulating Hormone (TSH)" and score >= self.attribute_thresholds['Thyroid Stimulating Hormone (TSH)']:
                    post_process_test_result, test_name_unit = Postprocess.tsh_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Free Thyroxine (FT4)" and score >= self.attribute_thresholds['Free Thyroxine (FT4)']:
                    post_process_test_result, test_name_unit = Postprocess.ft4_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Thyroxine (T4)" and score >= self.attribute_thresholds['Thyroxine (T4)']:
                    post_process_test_result, test_name_unit = Postprocess.t4_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Triiodothyronine (FT3)" and score >= self.attribute_thresholds['Triiodothyronine (FT3)']:
                    post_process_test_result, test_name_unit = Postprocess.ft3_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Relative Lymphocytes" and score >= self.attribute_thresholds['Relative Lymphocytes']:
                    post_process_test_result, test_name_unit = Postprocess.rel_lymph_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Absolute Lymphocytes" and score >= self.attribute_thresholds['Absolute Lymphocytes']:
                    post_process_test_result, test_name_unit = Postprocess.abs_lymph_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Lactate Dehydrogenase (LDH)" and score >= self.attribute_thresholds['Lactate Dehydrogenase (LDH)']:
                    post_process_test_result, test_name_unit = Postprocess.lld_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Uric Acid, Serum" and score >= self.attribute_thresholds['Uric Acid, Serum']:
                    post_process_test_result, test_name_unit = Postprocess.uric_acid_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Uric Acid, Urine" and score >= self.attribute_thresholds['Uric Acid, Urine']:
                    post_process_test_result, test_name_unit = Postprocess.uric_acid_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "PaO2, Arterial" and score >= self.attribute_thresholds['PaO2, Arterial']:
                    post_process_test_result, test_name_unit = Postprocess.paO2_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "PaCO2" and score >= self.attribute_thresholds['PaCO2']:
                    post_process_test_result, test_name_unit = Postprocess.paCO2_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "HCO3" and score >= self.attribute_thresholds['HCO3']:
                    post_process_test_result, test_name_unit = Postprocess.HCO3_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Glucose" and score >= self.attribute_thresholds['Glucose']:
                    post_process_test_result, test_name_unit = Postprocess.glucose_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Calcium" and score >= self.attribute_thresholds['Calcium']:
                    post_process_test_result, test_name_unit = Postprocess.calcium_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Magnesium" and score >= self.attribute_thresholds['Magnesium']:
                    post_process_test_result, test_name_unit = Postprocess.magnesium_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Phosphate" and score >= self.attribute_thresholds['Phosphate']:
                    post_process_test_result, test_name_unit = Postprocess.phosphate_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "Carbon Dioxide" and score >= self.attribute_thresholds['Carbon Dioxide']:
                    post_process_test_result, test_name_unit = Postprocess.CO2_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)
                elif attr == "PTT" and score >= self.attribute_thresholds['PTT']:
                    post_process_test_result, test_name_unit = Postprocess.ptt_postprocess(test_result)
                    test_name_ = attr
                    ref_range, abnormal_flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                        test_name_, post_process_test_result, self.test_name_range_dict)

                else:
                    pass

                post_process_date = p_obj.date_parser(
                    str(date), (default_year, 1, 1))

                if post_process_test_result_ and test_name_ and post_process_date:
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '', raw_test_name,raw_test_result,raw_test_date,test_name_, post_process_test_result,
                            test_name_unit, ref_range, post_process_date, abnormal_flag, date_from, section, sub_section, main_section, is_from, bb_info, row['IsDrop'])))
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '', raw_test_name,raw_test_result,raw_test_date,test_name__, post_process_test_result_,
                            test_name_unit, ref_range_, post_process_date, abnormal_flag_, date_from, section, sub_section, main_section, is_from, bb_info, row['IsDrop'])))
                elif post_process_test_result and test_name_ and post_process_date:
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '', raw_test_name,raw_test_result,raw_test_date,test_name_, post_process_test_result,
                            test_name_unit, ref_range, post_process_date, abnormal_flag, date_from, section, sub_section, main_section, is_from, bb_info, row['IsDrop'])))
                else:                    
                    post_process_data.append(self.get_model_data(
                        data=(doc_name, page_no, '', raw_test_name,raw_test_result,raw_test_date,attr, post_process_test_result,
                            '', ref_range, post_process_date, abnormal_flag, date_from, section, sub_section, main_section, is_from, bb_info, 1)))
        return post_process_data

    def get_postprocess_data_for_excerpt(self, pre_df, p_obj, default_year, attribute_ls, regex_attribute_list):
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
            if row['TestName'] == 'Fever':
                post_process_test_result = Postprocess.temperature_postprocess(
                    row['TestResult'])
                ref_range, flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = 'Fahrenheit'
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'Procalcitonin':
                post_process_test_result, test_name_unit = Postprocess.procalcitonin_postprocess(
                    row['TestResult'])
                ref_range, flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'C-Reactive Protein (CRP)':
                post_process_test_result, test_name_unit = Postprocess.crp_postprocess(
                    row['TestResult'])
                ref_range, flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'WBC':
                post_process_test_result, test_name_unit = Postprocess.wbc_postprocess(
                    row['TestResult'])
                ref_range, flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'Bands (i.e. left-shift)':
                post_process_test_result, test_name_unit = Postprocess.imm_gran_postprocess(
                    row['TestResult'])
                ref_range, flag = PneumoniaAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Urine Volume (e.g. I&O\'s)":
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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Baseline Serum Creatinine":
                post_process_test_result, test_unit = Postprocess.baseline_creatinine_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = 0
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Cardiac Troponin":
                post_process_test_result, test_unit = Postprocess.trop_postprocess(
                    row['TestResult'])
                ref_range, flag = AMIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'Mean Arterial Pressure':
                post_process_test_result, test_name_unit = Postprocess.map_postprocess(
                    row['TestResult'])
                ref_range, flag = VitalAnalytics.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.vital_ref_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'PT':
                post_process_test_result, test_name_unit = Postprocess.pt_postprocess(
                    row['TestResult'])
                ref_range, flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'INR':
                post_process_test_result, test_name_unit = Postprocess.inr_postprocess(
                    row['TestResult'])
                ref_range, flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'APTT':
                post_process_test_result, test_name_unit = Postprocess.aptt_postprocess(
                    row['TestResult'])
                ref_range, flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == 'Platelets':
                post_process_test_result, test_name_unit = Postprocess.platelets_postprocess(
                    row['TestResult'])
                ref_range, flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result, self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

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
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Glasgow Coma Scale (GCS)":
                post_process_test_result, test_name_unit = Postprocess.gcs_postprocess(
                    row['TestResult'])
                ref_range, flag = SepsisAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Thyroid Stimulating Hormone (TSH)":
                post_process_test_result, test_name_unit = Postprocess.tsh_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Glucose":
                post_process_test_result, test_name_unit = Postprocess.glucose_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Magnesium":
                post_process_test_result, test_name_unit = Postprocess.magnesium_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Uric Acid, Serum":
                post_process_test_result, test_name_unit = Postprocess.uric_acid_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Uric Acid, Urine":
                post_process_test_result, test_name_unit = Postprocess.uric_acid_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
                
            elif row['TestName'] == "Alcohol Level":
                post_process_test_result, test_name_unit = Postprocess.blood_alcohol_level_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
                
            elif row['TestName'] == "Thiamine Test":
                post_process_test_result, test_name_unit = Postprocess.vitamin_B1_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Vitamin B12":
                post_process_test_result, test_name_unit = Postprocess.vitamin_B12_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Ammonia Level Test":
                post_process_test_result, test_name_unit = Postprocess.ammonia_level_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Lactate Dehydrogenase (LDH)":
                post_process_test_result, test_name_unit = Postprocess.lld_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Thyroxine (T4)":
                post_process_test_result, test_name_unit = Postprocess.t4_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Triiodothyronine (FT3)":
                post_process_test_result, test_name_unit = Postprocess.ft3_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "HCO3":
                post_process_test_result, test_name_unit = Postprocess.HCO3_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Acetaminophen Test":
                post_process_test_result, test_name_unit = Postprocess.acetaminophen_level_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop'] 

            elif row['TestName'] == "Free Thyroxine (FT4)":
                post_process_test_result, test_name_unit = Postprocess.ft4_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
                
            elif row['TestName'] == "RBC":
                post_process_test_result, test_name_unit = Postprocess.rbc_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Aspartate Transaminase (AST)":
                post_process_test_result, test_name_unit = Postprocess.ast_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Alanine Transaminase (ALT)":
                post_process_test_result, test_name_unit = Postprocess.alt_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Serum Cortisol":
                post_process_test_result, test_name_unit = Postprocess.serum_cortisol_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Hemoglobin Test":
                post_process_test_result, test_name_unit = Postprocess.hgb_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Hematocrit Test":
                post_process_test_result, test_name_unit = Postprocess.hematocrit_test_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            
            elif row['TestName'] == "Bilirubin":
                post_process_test_result, test_name_unit = Postprocess.bilirubin_postprocess(
                    row['TestResult'])
                ref_range, flag = AKIAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Glucose":
                post_process_test_result, test_name_unit = Postprocess.glucose_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Magnesium":
                post_process_test_result, test_name_unit = Postprocess.magnesium_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Alkaline Phosphatase":
                post_process_test_result, test_name_unit = Postprocess.alkaline_phosphate_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            elif row['TestName'] == "Albumin":
                post_process_test_result, test_name_unit = Postprocess.albumin_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Total Protein":
                post_process_test_result, test_name_unit = Postprocess.total_protein_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "Calcium":
                post_process_test_result, test_name_unit = Postprocess.calcium_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']

            elif row['TestName'] == "GFR Test":
                post_process_test_result, test_name_unit = Postprocess.gfr_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
            elif row['TestName'] == "Carbon Dioxide":
                post_process_test_result, test_name_unit = Postprocess.CO2_postprocess(
                    row['TestResult'])
                ref_range, flag = EncephalopathyAnalysis.get_ref_range_and_boolean_flag(
                    row['TestName'], post_process_test_result , self.test_name_range_dict)
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = flag
                pre_df.at[i, 'ReferenceRange'] = ref_range
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = test_name_unit
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
             
            elif row['TestName'] in attribute_ls:
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = 0 if row['isNegated'] else 1
                # pre_df.at[i, 'IsAbnormal'] = row['IsNegation']
                pre_df.at[i, 'ReferenceRange'] = ''
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = ''
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
            
            else:
                pre_df.at[i, 'TestResult'] = post_process_test_result
                pre_df.at[i, 'IsAbnormal'] = None
                pre_df.at[i, 'ReferenceRange'] = ''
                pre_df.at[i, 'TestDateTime'] = post_process_date
                pre_df.at[i, 'TestUnit'] = ''
                pre_df.at[i, 'BBInfo'] = bb_info
                pre_df.at[i, 'IsDrop'] = row['IsDrop']
                
        numeric_df_ = pd.DataFrame()
        if pre_df.shape[0]>0:
            numeric_df = pre_df[pre_df['TestName'].isin(regex_attribute_list)]
            pre_df['TestDateTime'] = pre_df['TestDateTime'].apply(lambda x: p_obj.date_parser(
                str(x), (default_year, 1, 1))).to_list()
            pre_df['TestDateTime'] = pre_df['TestDateTime'].apply(
                lambda x: x if x else '')
            
            numeric_df_ = numeric_df[['DocumentName', 'Page', 'DocumentLink','RawTestName','RawTestResult','RawTestDateTime','TestName', 'TestResult', 'TestUnit', 'ReferenceRange', 'TestDateTime', 'IsAbnormal', 'Section', 'SubSection', 'MainSection',
                                      'DateFrom', 'IsFrom', 'BBInfo', 'IsDrop']]
        logging.info(f'FUNCTION END : get_postprocess_data_for_excerpt : Excerpts shape : {pre_df.shape}')
        logging.info(f'FUNCTION END : get_postprocess_data_for_excerpt : Excerpts via regex shape  : {numeric_df_.shape}')
        return pre_df, numeric_df_

    def get_postprocess_df(self, s3_c, bucket_name, save_path, post_process_obj, default_year, raw_data_df, raw_data_excerpt_df, attribute_ls, regex_attribute_list, path_to_save_logs):
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
        logging.info(f'FUNCTION START')
        postprocess_df, postprocess_df_output_file, post_process_excerpt_df, post_process_excerpt_df_output_file = pd.DataFrame(),None, pd.DataFrame(), None
        final_post_process_df, final_post_process_excerpt_df = pd.DataFrame(), pd.DataFrame()
        if raw_data_df.shape[0] > 0 or raw_data_excerpt_df.shape[0] > 0:
            post_process_data = self.get_postprocess_data(
                raw_data_df, post_process_obj, default_year)
            post_process_excerpt_df, post_process_num_excerpt_df = self.get_postprocess_data_for_excerpt(
                raw_data_excerpt_df, post_process_obj,
                default_year, attribute_ls, regex_attribute_list
            )
            if not post_process_num_excerpt_df.empty:
                post_process_num_excerpt_df['BBInfo'] = post_process_num_excerpt_df['BBInfo'].apply(lambda x: align_bb(eval(x)[0][0]) if eval(x)[0] else (None, None, None, None))
            postprocess_df = pd.DataFrame(post_process_data)
            logging.info(f'post_process_data shape : {postprocess_df.shape}')
            if len(post_process_data) > 0:
                # postprocess_csv_path = rf"{save_path}/{str(postprocess_df['DocumentName'].iloc[0]).replace('.csv','')}_postprocessTableEntity.csv"
                table_entity_file_name = rf"{save_path.split('/')[-1]}_postprocessTableEntity.csv"
                table_entity_suppress_file_name = rf"{save_path.split('/')[-1]}_postprocessSuppressTableEntity.csv"
                postprocess_df = pd.concat(
                    [postprocess_df, post_process_num_excerpt_df], ignore_index=True)
                logging.info(f'post_process_data + post_process_num_excerpt_df shape : {postprocess_df.shape}')
                sbp_dbp_df = self.suppress_sbp_dbp(postprocess_df)
                dff_ = postprocess_df[~postprocess_df['TestName'].isin(['Blood Pressure - Diastolic',
                                                  'Blood Pressure - Systolic'])]
                postprocess_df_ = pd.concat(
                    [dff_, sbp_dbp_df], ignore_index=True)
                logging.info(f'post_process_data excluding sbp_dbp shape : {dff_.shape}')
                logging.info(f'sbp_dbp_df shape : {sbp_dbp_df.shape}')
                logging.info(f'post_process_data + sbp_dbp_df shape : {postprocess_df.shape}')
                postprocess_df = suppress_value(postprocess_df_)

                postprocess_df, excp_intersect_df = suppress_test_over_tableRegion(postprocess_df.copy())

                postprocess_df = self.suppress_ed_notes(postprocess_df)
                postprocess_df.index = pd.RangeIndex(postprocess_df.shape[0])
                
                postprocess_df['DocumentName'] = [
                    f"{save_path.split('/')[-1]}"]*postprocess_df.shape[0]
                postprocess_df['DocumentLink'] = postprocess_df.apply(
                    lambda x: f"{x['DocumentName']}_highlighted.pdf#page={x['Page']}", axis=1)
                
                idx = postprocess_df[(postprocess_df['TestName'] == 'Baseline Serum Creatinine') & \
                        (postprocess_df['TestResult'] != postprocess_df[postprocess_df['TestName'] == 'Baseline Serum Creatinine']['TestResult'].max())].index
                postprocess_df.loc[idx, 'IsDrop'] = 1

                final_post_process_df = postprocess_df[postprocess_df['IsDrop'] == 0]
                final_post_process_suppress_df = postprocess_df[postprocess_df['IsDrop'] == 1]
                final_post_process_df.reset_index(inplace=True)
                final_post_process_suppress_df.reset_index(inplace=True)

                put_object_to_s3(s3_c, bucket_name, save_path,
                                 table_entity_file_name, final_post_process_df)
                
                put_object_to_s3(s3_c, bucket_name, path_to_save_logs,
                                 table_entity_suppress_file_name, final_post_process_suppress_df)        

                # postprocess_df.to_csv(
                #     postprocess_csv_path, index=False)
                postprocess_df_output_file = f"{save_path}/{table_entity_file_name}"
                logging.info(
                    'concatenate table df and numeric df from excerpts ...')
            else:
                logging.info('No postprocess data generated for table ...')
            if post_process_excerpt_df.shape[0] > 0:
                excerpt_entity_file_name = rf"{save_path.split('/')[-1]}_postprocessExcerptEntity.csv"
                suppress_excerpt_entity_file_name = rf"{save_path.split('/')[-1]}_postprocessSuppressExcerptEntity.csv"
                
                post_process_excerpt_df['DocumentName'] = [
                    f"{save_path.split('/')[-1]}"]*post_process_excerpt_df.shape[0]
                post_process_excerpt_df['DocumentLink'] = post_process_excerpt_df.apply(
                    lambda x: f"{x['DocumentName']}_highlighted.pdf#page={x['Page']}", axis=1)
                
                final_post_process_excerpt_df = post_process_excerpt_df[post_process_excerpt_df['IsDrop'] == 0]
                final_post_process_excerpt_suppress_df = post_process_excerpt_df[post_process_excerpt_df['IsDrop'] == 1]
                final_post_process_excerpt_df.reset_index(inplace=True)
                final_post_process_excerpt_suppress_df.reset_index(inplace=True)
                
                put_object_to_s3(s3_c, bucket_name, save_path,
                                 excerpt_entity_file_name, final_post_process_excerpt_df)
                
                put_object_to_s3(s3_c, bucket_name, path_to_save_logs,
                                 suppress_excerpt_entity_file_name, final_post_process_excerpt_suppress_df)
                
                post_process_excerpt_df_output_file = f"{save_path}/{excerpt_entity_file_name}"

            else:
                logging.info('No postprocess data generated for excerpt ...')

        else:
            logging.info('No raw_data_df found...')
        return final_post_process_df, postprocess_df_output_file, final_post_process_excerpt_df, post_process_excerpt_df_output_file
