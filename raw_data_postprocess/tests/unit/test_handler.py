import json
import logging
import boto3
import os
from rapidfuzz import process, fuzz

# logging.basicConfig(level=logging.DEBUG)
try:
    from raw_data_postprocess.utility.table_parser import TableParser
    from raw_data_postprocess.utility.table_merger_v2 import TableMerger
    from raw_data_postprocess.postprocess.post_process import Postprocess
    from raw_data_postprocess.constant.aws_config import aws_access_key_id, aws_secret_access_key
except ModuleNotFoundError as e:
    from ...raw_data_postprocess.utility.table_parser import TableParser
    from ...raw_data_postprocess.utility.table_merger_v2 import TableMerger
    from ...raw_data_postprocess.postprocess.post_process import Postprocess
    from ...raw_data_postprocess.constant.aws_config import aws_access_key_id, aws_secret_access_key
# from raw_data_postprocess.utility.table_parser import TableParser

import pandas as pd
import unittest
s3_c = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
# s3_c = boto3.client('s3', region_name='us-east-1')
lab_constant_object = s3_c.get_object(Bucket='zai-revmax-develop',
                                      Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/lab_test_constant.json')
lab_constant_body = lab_constant_object["Body"].read().decode('utf-8')
lab_constant = json.loads(lab_constant_body)

lab_master_list = lab_constant.get('Vital').get('lab_master_list')
lab_alias_dict = lab_constant.get('Vital').get('lab_alias_dict')
lab_unit_list = lab_constant.get('Vital').get('lab_unit_list')

component_dict = {
    'HeartRate': ['peripheral pulse rate', 'pulse rate', 'hr', 'heart rate', 'pulse'],
    'BloodPressure': ['systolicbloodpressure', 'diastolicbloodpressure', 'blood pressure', 'systolic blood pressure', 'diastolic blood pressure', 'bp', 'b/p', 'sbp', 'dbp', 'nibp mg'],
    'Spo2': ['spo2', 'pulse ox', 'pulse oximeter'],
    'Temperature': ['temp', 'temperature', 'temperature oral'],
    'O2FlowRate': ['o2 flow rate', 'flow rate'],
    'RespiratoryRate': ['respiratoryrate', 'resp', 'resp rate', 'rr', 'respiratory rate',],

    'Sodium': ['na lvl', 'sodium lvl', 'na', 'sodium',],
    'Potassium': ['potassium lvl', 'k lvl', 'potassium', 'k',],
    'Creatinine': ['creatinine lvl', 'creat lvl', 'creatinine', 'cret'],
    'BUN': ['bun lvl', 'blood urea nitrogen', 'bun'],
    'Bilirubin': ['bili total', 'bilirubin'],
    'Urine_specific_gravity': ['urine specific gravity', 'ua spec grav', 'ua spgr', 'ua sg'],
    'Urine_osmolality': ['osmol random u', 'ur osmolality'],
    'Urine_sodium': ['sodium random urine', 'rndm sodiumm ua', 'random na ur', 'na rndm urine'],
    'Urine_creatinine': ['creatinine random urine', 'rndm creat ua'],
    'Urine_protein': ['urine protein - dipstick', 'ur prot', 'totl urine protein'],
}


class TestTableParser(unittest.TestCase):
    def test_pattern_checker_1a(self):
        # df_path = r"tests\test_cases\table_parser\date_header_in_1st_row_with_random_multiple_column.csv"
        df_path = r"tests\test_cases\table_parser\date_header_in_1st_row_with_random_multiple_column.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)
        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag,df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag, True)

    def test_pattern_checker_1b(self):
        df_path = r"tests\test_cases\table_parser\date_header_in_2_row.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)
        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag, True)

    def test_pattern_checker_1c(self):
        df_path = r"tests\test_cases\table_parser\single_date_header_in_row_with_comp_in_1st_col.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)
        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag, True)

    def test_pattern_checker_1d(self):
        df_path = r"tests\test_cases\table_parser\all_header_containing_comp_and_date_in_zeroth_col.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1e(self):
        df_path = r"tests\test_cases\table_parser\date_header_in_2nd_row_with_random_column.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1f(self):
        df_path = r"tests\test_cases\table_parser\header_with_datetime_1st_col_has_comp.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1g(self):
        df_path = r"tests\test_cases\table_parser\first_col_empty_2nd_contains_time_and_header_has_components.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df= vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1h(self):
        df_path = r"tests\test_cases\table_parser\2nd_row_contains_header_and_1st_col_has_date.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1i(self):
        df_path = r"tests\test_cases\table_parser\date_in_last_col_comp_in_2nd_row.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1,df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_1j(self):
        df_path = r"tests\test_cases\table_parser\false_date_present_in_first_row.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1 , df= vitalTableParserObj.pattern_1_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, True)

    def test_pattern_checker_2a(self):
        df_path = r"tests\test_cases\table_parser\verticle_table_with_comp_date_val_column.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1, df = vitalTableParserObj.pattern_1_checker(pre_process_df)
        pattern_flag_2 = vitalTableParserObj.pattern_2_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, False)
        self.assertEqual(pattern_flag_2, {
                         'lab_test_col': 'lab_name', 'date_col': 'Date Time', 'text': 'Result'})

    def test_pattern_checker_2b(self):
        df_path = r"tests\test_cases\table_parser\all_the_headers_has_date_and_comp_in_last_row.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1 = vitalTableParserObj.pattern_2a_checker(pre_process_df)
        data_ls = vitalTableParserObj.get_extracted_value_for_pattern_2(
            vitalTableParserObj.transpose_df(pre_process_df))
        self.assertEqual(pattern_flag_1, True)
        self.assertEqual(len(data_ls), 5)

    def test_pattern_checker_2c(self):
        df_path = r"tests\test_cases\table_parser\with_range_and_unit_col.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1 = vitalTableParserObj.pattern_2_checker(pre_process_df)
        self.assertEqual(pattern_flag_1, {'lab_test_col': 'Procedure ',
                                          'unit_col': 'Units ',
                                          'date_col': 'Result Date/Time ',
                                          'text': 'Result '})
        # self.assertEqual(len(data_ls), 5)

    def test_pattern_checker_2d(self):
        df_path = r"tests\test_cases\table_parser\all_the_headers_has_comp_and_date_in_last_row.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        pattern_flag_1 = vitalTableParserObj.pattern_2a_checker(pre_process_df)
        data_ls = vitalTableParserObj.get_extracted_value_for_pattern_2(
            vitalTableParserObj.transpose_df(pre_process_df))
        self.assertEqual(pattern_flag_1, True)
        self.assertEqual(len(data_ls), 5)

    def test_pattern_checker_2e(self):
        df_path = r"tests\test_cases\table_parser\less_number_of_comp_and_unkown_result_col.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        column_dict = vitalTableParserObj.pattern_2_checker(pre_process_df)
        # data_ls = vitalTableParserObj.get_extracted_value_for_pattern_2(
        #     vitalTableParserObj.transpose_df(pre_process_df))
        self.assertEqual(column_dict, {
                         'lab_test_col': 'Procedure ', 'unit_col': 'Units ', 'text': '1'})

    def test_pattern_checker_2f(self):
        df_path = r"tests\test_cases\table_parser\null_header_for_pattern_2.csv"
        dff = pd.read_csv(df_path)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_process_df = vitalTableParserObj.preprocess_df(dff)
        column_dict = vitalTableParserObj.pattern_2_checker(pre_process_df)
        # data_ls = vitalTableParserObj.get_extracted_value_for_pattern_2(
        #     vitalTableParserObj.transpose_df(pre_process_df))
        print(column_dict)
        self.assertEqual(
            column_dict, {'date_col': '0', 'lab_test_col': 'lab_name', 'text': '2'})

    def test_pattern_checker_multiheader_1a(self):
        df_path = r"tests\test_cases\table_parser\multiheader_date_table.csv"
        dff = pd.read_csv(df_path,na_filter=True)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        # pre_dff = vitalTableParserObj.preprocess_df(dff.copy())
        # pt1_flag = vitalTableParserObj.pattern_1_checker(pre_dff.copy())
        # sub_df_list = vitalTableParserObj.get_multiheader_table_df(
        #     pre_dff.copy())
        # res1 = vitalTableParserObj.get_extracted_value_for_pattern_1(
        #     sub_df_list[0])
        # logging.debug(res1)
        res_list = vitalTableParserObj.get_result_from_table_csv(dff)
        conct_res = []
        for sbr in res_list:
            if str(sbr.get('name')).strip() in ['SpO2', 'BP', 'Resp', 'Pulse', 'Temp']:
                conct_res.append(sbr)
        
        self.assertEqual(
            75, len(conct_res))

    def test_pattern_checker_multiheader_1b(self):
        df_path = r"tests\test_cases\table_parser\multiheader_table_count.csv"
        dff = pd.read_csv(df_path,na_filter=True)
        vitalTableParserObj = TableParser(lab_master_list,
                                          lab_alias_dict, lab_unit_list)

        pre_dff = vitalTableParserObj.preprocess_df(dff.copy())
      
        sub_df_list = vitalTableParserObj.get_multiheader_table_df(
            pre_dff.copy())
        self.assertEqual(
            3, len(sub_df_list))
    
    # def test_table_merging(self):
    #     test_case_path = r"tests\test_cases\table_merger"
    #     table_csv_list = os.listdir(test_case_path)

    #     table_csv_list_ordered = sorted(table_csv_list, key=lambda x: int(
    #         x.split("/")[-1].split("_")[0]))

    #     table_csv_df_map_dict = {k.split("/")[-1]: pd.read_csv(rf"{test_case_path}/{k}")
    #                             for k in table_csv_list_ordered}
    #     merged_table_metadata_lab = TableMerger(s3_c,
    #                                             '',
    #                                             table_csv_df_map_dict,
    #                                             table_csv_list_ordered,
    #                                             lab_constant.get('CBC').get(
    #                                                 'lab_master_list'),
    #                                             {}).get_merger_meta_data()
        
    #     true_case = {(180,), (180, 181), (181,)}
    #     flag = len(true_case.symmetric_difference(
    #         set([i[0] for i in merged_table_metadata_lab])))
    #     self.assertEqual(flag, 0)

    def test_postprocess(self):
        test_case_df = pd.read_csv(
            r'tests\test_cases\post_process\postprocess_test_case.csv')
        def get_test_name(test_name):
            return_test_name = test_name
            for cmp_, key_terms in component_dict.items():
                matcher_1 = process.extractOne(
                    test_name.strip().lower(), key_terms, scorer=fuzz.token_sort_ratio)
                matcher_2 = process.extractOne(
                    test_name.strip().lower(), key_terms, scorer=fuzz.partial_token_sort_ratio)
                score = (matcher_1[1]+matcher_2[1])/2
                if score >= 76:
                    return_test_name = cmp_
            return return_test_name

        test_case_df['TestNameProcessed'] = test_case_df['TestName'].apply(
            lambda x: get_test_name(str(x)))
        
        testNameList, testResultList = [],[]
        testNameProcess, testResultProcess = None,None
        for i, row in test_case_df.iterrows():
            testName,testResult = row['TestNameProcessed'], row['TestResult']
            if testName =='Sodium':
                testResult_, unit = Postprocess.sodium_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName == 'Potassium':
                testResult_, unit = Postprocess.potassium_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName == 'BUN':
                testResult_, unit = Postprocess.bun_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName == 'Creatinine':
                testResult_, unit = Postprocess.creatinine_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName == 'Bilirubin':
                testResult_, unit = Postprocess.bilirubin_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName == 'Immature_gran':
                testResult_, unit = Postprocess.imm_gran_postprocess(testResult)
            elif testName == 'WBC':
                testResult_, unit = Postprocess.wbc_postprocess(testResult)
                testResultProcess, testNameProcess =testResult_, testName
            elif testName =='Temperature':
                testResult_ = Postprocess.temperature_postprocess(testResult)
                testResultProcess, testNameProcess = testResult_, testName
            elif testName == 'HeartRate':
                testResult_ = Postprocess.pulse_rate_postprocess(testResult)
                testResultProcess, testNameProcess = testResult_, testName
            elif testName == 'RespiratoryRate':
                testResult_ = Postprocess.respiratory_rate_postprocess(testResult)
                testResultProcess, testNameProcess = testResult_, testName
            elif testName == 'Spo2':
                testResult_ = Postprocess.spo2_postprocess(testResult)
                testResultProcess, testNameProcess = testResult_, testName
            elif testName == 'O2FlowRate':
                testResult_ = Postprocess.o2_flow_rate_postprocess(testResult)
                testResultProcess, testNameProcess = testResult_, testName
            
            if testNameProcess and testResultProcess:
                testNameList.append(testNameProcess)
                testResultList.append(testResultProcess)
                testNameProcess, testResultProcess = None, None

        df_result = pd.DataFrame({'TestName': testNameList,
                                  'TestResult': testResultList})
        result_dict = df_result['TestName'].value_counts().to_dict()
        self.assertEqual(result_dict, {'Temperature': 2, 'Sodium': 1, 'BUN': 2,
                                       'Bilirubin': 2,
                                       'Creatinine': 2,
                                       'HeartRate': 1,
                                       'O2FlowRate': 2,
                                       'Potassium': 1, })

if "__name__" == "__main__":
    unittest.main()

# def test_lambda_handler(apigw_event):

#     ret = app.lambda_handler(apigw_event, "")
#     data = json.loads(ret["body"])

#     assert ret["statusCode"] == 200
#     assert "message" in ret["body"]
#     assert data["message"] == "hello world"

# def test_lambda_handler(apigw_event):

#     ret = app.lambda_handler(apigw_event, "")
#     data = json.loads(ret["body"])

#     assert ret["statusCode"] == 200
#     assert "message" in ret["body"]
#     assert data["message"] == "hello world"