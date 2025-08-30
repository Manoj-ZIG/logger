import json
import unittest
import pandas as pd
import boto3
from io import StringIO

from vital_lab_extraction.get_lab_extraction import get_lab_metadata
from vital_lab_extraction.utility.excerpt_extraction import ExcerptExtraction
from vital_lab_extraction.constant.template_excerpt_constant import template_excerpt_data
from vital_lab_extraction.constant.date_tag_constant import date_tags, suppress_date_tag
from vital_lab_extraction.constant.aws_config import aws_access_key_id, aws_secret_access_key

# constant
# s3_c = boto3.client('s3', aws_access_key_id=aws_access_key_id,
#                     aws_secret_access_key=aws_secret_access_key)
s3_c = boto3.client('s3', region_name='us-east-1')
lab_constant_object = s3_c.get_object(Bucket='zai-revmax-develop',
                                          Key='mr_data/lab_extraction/lab_extraction_constant/supporting_file_constant/lab_test_constant.json')
lab_constant_body = lab_constant_object["Body"].read().decode('utf-8')
lab_constant = json.loads(lab_constant_body)

class TestStringMethods(unittest.TestCase):

    def test_detected_page(self):
       
        bucket_name = 'zai-revmax-develop'
        sec_subsec_csv = "mr_data/lab_extraction/lab_extraction_constant/unittest_constant/test_section_subsection_2299H6841.csv"
        # sec_subsec_csv = rf"E:\NLP_revMaxAI\scripts\sam_cli_test\vital_lab_exctraction\vital_lab_extraction\tests\constant\test_section_subsection_2299H6841.csv"
        file = "test_section_subsection_2299H6841.csv"

        # constant_path = 'mr_data/section_subsection/section_subsection_constant/supporting_file_constant'
        lab_extraction_data = get_lab_metadata(bucket_name,
            sec_subsec_csv, file, '', detected_labs=['blood_culture','sputum'])
        test_case = {"vital": [9, 19, 20, 26, 30, 36, 42, 49, 54, 58, 60, 67, 68, 69, 75, 80, 81, 82, 87, 89, 93, 95, 103, 108, 110, 115, 116, 121, 123, 127, 130, 135, 138, 145, 150, 151, 154, 160, 165, 167, 170, 176, 182, 184, 188, 190, 197, 199, 203, 207, 221, 223], "cbc": [9, 32, 55, 61, 70, 76, 83,
                                                                                                                                                                                                                                                                                     139, 146, 147, 156, 161, 168, 179, 185, 190, 193, 211, 212, 266, 268, 271], "cmp": [22, 29, 33, 43, 54, 55, 61, 63, 70, 77, 83, 84, 89, 95, 104, 111, 117, 123, 131, 139, 141, 146, 147, 155, 156, 161, 167, 168, 171, 178, 179, 184, 185, 191, 194, 199, 200, 204, 211, 212], "blood_culture": [268]}
        vital_page_list = lab_extraction_data.get('vital').get('page_list')
        cbc_page_list = lab_extraction_data.get('cbc').get('page_list')
        cmp_page_list = lab_extraction_data.get('cmp').get('page_list')
        blood_cult_page_list = lab_extraction_data.get('blood_culture').get('page_list')

        vital_check = set(vital_page_list).difference(test_case.get('vital'))
        cbc_check = set(cbc_page_list).difference(test_case.get('cbc'))
        cmp_check = set(cmp_page_list).difference(test_case.get('cmp'))
        blood_cult_check = set(blood_cult_page_list).difference(
            test_case.get('blood_culture'))
        self.assertEqual(len(vital_check), 0)
        self.assertEqual(len(cbc_check), 0)
        self.assertEqual(len(cmp_check), 0)
        self.assertEqual(len(blood_cult_check), 0)

    def test_excerpt_count(self):
        sec_subsec_csv = "mr_data/section_subsection/section_subsection_test_mr/output_mr_test_sec_subsection_csv/section_subsection_229954040.309398918.H68414674.csv"
        textract_csv_path = "mr_data/input_data/229954040.309398918.H68414674.csv"
        bucket_name = 'zai-revmax-develop'
        attribute_dict = template_excerpt_data.get(
            'pneumonia').get('attribute_dict')
        attribute_regex_dict = template_excerpt_data.get(
            'pneumonia').get('attribute_regex_dict')
        excerpt_obj = ExcerptExtraction(
            sec_subsec_csv, textract_csv_path, bucket_name,
            date_tags,
            suppress_date_tag, 'pneumonia',
            attribute_dict, attribute_regex_dict)

        excerpt_result = excerpt_obj.get_excerpts()
        excerpt_result_df = excerpt_obj.get_result_df(excerpt_result)
        result = excerpt_result_df['TestName'].value_counts().to_dict()
        test_case = {'Mechanical Ventilation': 61,
                     'CMP': 61,
                     'CBC': 47,
                     'Antiviral_Antifungal_Antibiotics': 47,
                     'Fever': 29,
                     'Pneumonia': 28,
                     'Cough': 22,
                     'Leukocytosis': 22,
                     'Rhonchi': 21,
                     'Chest xray (CXR)': 19,
                     'Blood Culture': 14,
                     'Sputum Culture': 14,
                     'Gram Stain': 12,
                     'Procalcitonin': 9,
                     'Tachycardia': 8,
                     'PCR Rapid Influenza': 7,
                     'Shortness of Breath': 5,
                     'Supplemental Oxygen': 4}
        flag = []
        for k, v in test_case.items():
            if result.get(k):
                flag.append(result.get(k) >= v)
            else:
                flag.append(False)
        self.assertEqual(all(flag), True)

if __name__ == '__main__':
    unittest.main()


# def test_lambda_handler(apigw_event):

#     ret = app.lambda_handler(apigw_event, "")
#     data = json.loads(ret["body"])

#     assert ret["statusCode"] == 200
#     assert "message" in ret["body"]
#     assert data["message"] == "hello world"
