import json
import boto3
import unittest
import pandas as pd
from io import StringIO

try:
    from section_subsection.utility.section_subsection_detection import Section
    from section_subsection.constant.aws_config import aws_access_key_id, aws_secret_access_key
except:
    from ...section_subsection.utility.section_subsection_detection import Section
    from ...section_subsection.constant.aws_config import aws_access_key_id, aws_secret_access_key

    
class TestStringMethods(unittest.TestCase):

    def test_Physician_Name(self):
        
        # self.s3_c = boto3.client('s3', aws_access_key_id=aws_access_key_id,
        #                          aws_secret_access_key=aws_secret_access_key)
        self.s3_c = boto3.client('s3', region_name='us-east-1')
        test_case_obj = self.s3_c.get_object(
            Bucket='zai-revmax-develop', Key="mr_data/section_subsection/section_subsection_constant/unittest_support_file/Physician_Name_Test_Cases.csv")
        test_case_body = test_case_obj['Body']
        test_case_body_str = test_case_body.read().decode('utf-8')
        df = pd.read_csv(StringIO(test_case_body_str))

        File_name = ""
        textract_file = rf""

        constant_path = 'mr_data/section_subsection/section_subsection_constant/supporting_file_constant'
        obj = Section(textract_file, constant_path, File_name)
        # obj.section_end_info(df)
        # self.assertEqual(list(map(lambda x: str(x).upper(), df['Physician_Name'].to_list())) ==
        #                  obj.df_end['Physician Name'].to_list(), True)
        
        for idx in range(len(df)):
            with self.subTest(case=idx):
                ls = df.iloc[idx].values.tolist()
                col = df.iloc[idx].index.tolist()
                d_ = pd.DataFrame(ls).transpose()
                d_.columns = col
                obj.section_end_info(d_)
                self.assertEqual(df.iloc[idx]['Physician_Name'].upper() == obj.df_end.iloc[0]['Physician Name'], True)
                # if df.iloc[idx]['Physician_Name'].upper() != obj.df_end.iloc[0]['Physician Name']:
                #     with open(r"C:\Users\Rahuly\Downloads\phy_name.txt", 'a') as f:
                #         s = df.iloc[idx]['Physician_Name'].upper()+"|||"+ obj.df_end.iloc[0]['Physician Name'] + "\n"
                #         f.write(s)
                # if df.iloc[idx]['Physician_Name'].upper() != obj.df_end.iloc[0]['Physician Name']:
                #     print(df.iloc[idx]['Physician_Name'].upper(), obj.df_end.iloc[0]['Physician Name'])



if __name__ == '__main__':
    unittest.main()

# def test_lambda_handler(apigw_event):

#     ret = app.lambda_handler(apigw_event, "")
#     data = json.loads(ret["body"])

#     assert ret["statusCode"] == 200
#     assert "message" in ret["body"]
#     assert data["message"] == "hello world"
