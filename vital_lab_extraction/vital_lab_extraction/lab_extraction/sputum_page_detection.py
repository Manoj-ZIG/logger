import pandas as pd
import re
from rapidfuzz import fuzz, process
import boto3
from io import StringIO
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..constant.aws_config import aws_access_key_id, aws_secret_access_key

class Sputum:
    def __init__(self,bucket_name, csv_file, lab_test_list, lab_heading_list, master_section_list, master_subsection_list, master_exclusion_section_list, exclude_regex_str=None):
        self.bucket_name = bucket_name
        self.csv_file = csv_file
        self.lab_test_list = lab_test_list
        self.lab_heading_list = lab_heading_list
        self.exclude_regex_str = exclude_regex_str

        # master section/sub_section list for comparision
        self.master_section_list = master_section_list
        self.master_subsection_list = master_subsection_list
        self.master_exclusion_section_list = master_exclusion_section_list

    def get_corpus(self, d1):
        corpus = ' '
        for i, j in d1.iterrows():
            t1 = str(j['Text']).lower() + ' '
            corpus += t1
        return corpus

    def get_panel_date(self, df):
        """ 
        Function use case: sometime lab table don't have date associated with it, and in order to assign date to
                            respective lab value we can grab the date mentioned near to lab specific sub_section
        args:
            dataFrame: sub_dataFrame (by Page number)
            list: sub_section_list
        return:
            list: date_list
        """
        panel_dates = []
        idx_ls = []
        detected_sub_sec_ls = df['sub_section'].unique().tolist()
        for sub_sec_ in detected_sub_sec_ls:
            # check the matching score of detected sub_section against the provided sub_section list (Current Threshold= 72)
            score = process.extractOne(
                str(sub_sec_).lower().strip(), self.master_subsection_list, scorer=fuzz.token_sort_ratio)[1]
            if score >= 72:
                sub_sec_idx_ls = df[df['sub_section']
                                    == sub_sec_].index.to_list()
                for idx_ in sub_sec_idx_ls:
                    idx_ls.append(idx_)

        if len(idx_ls) > 0:
            for panel_idx in idx_ls:
                panel_idx_text = ' '.join(
                    df.loc[panel_idx-2:panel_idx+3, 'Text'].to_list())
                # panel_dates.append(get_date_time(panel_idx_text))
                panel_dates.append(
                    DatetimeExtractor.get_date_time_from_corpus_v2(panel_idx_text, ['dob']))
        # explode the list
        panel_dates = [item for sublist in panel_dates for item in sublist]
        return panel_dates

    def get_page_meta_data(self):
        """ 
        args:
            csv_file: csv (section subsection file)
            sub_section_list: list
            lab_test_list: list (for table detection)
            lab_heading_list: list (for table header detection)
            # excerpts_regex_str: str (for excerpts detection)
            exclude_regex_str: str (for exclude the pattern)
        return:
            meta_data: list [(page_no, section, subsection,table_element_match_list,header_match_list,panel_dates,corpus_dates,file_name)]
        """
        try:   
            self.s3_c = boto3.client('s3', region_name='us-east-1')
            self.s3_c.list_buckets()
            print("S3 client initialized successfully using IAM role in sputum_page_detection.py.")
        except Exception as e:
            print(f"Failed to initialize S3 client with IAM role: {str(e)} in sputum_page_detection.py.")
            if aws_access_key_id and aws_secret_access_key:
                self.s3_c = boto3.client('s3', 
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)
                print("S3 client initialized successfully using manual keys in sputum_page_detection.py.")
            else:
                raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in sputum_page_detection.py.")

        sec_subsection_csv_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=self.csv_file)
        sec_subsection_body = sec_subsection_csv_obj['Body']
        sec_subsection_string = sec_subsection_body.read().decode('utf-8')
        df = pd.read_csv(StringIO(sec_subsection_string), na_filter=False)
    
        # df = pd.read_csv(self.csv_file)
        pg_list = list(set(df['Page'].to_list()))
        meta_data = []

        for page in pg_list:
            d1 = df[df['Page'] == page]

            # corpus
            corpus = self.get_corpus(d1)
            pattern_for_table = r'\b(?:' + \
                '|'.join(map(re.escape, self.lab_test_list)) + r')\b'
            if self.exclude_regex_str:
                pattern_for_table = rf'(?<!{self.exclude_regex_str}\s)' + \
                    pattern_for_table

            # table element match
            table_element_match = re.findall(pattern_for_table, corpus, re.I)
            table_element_match_list = list(
                set([x for x in table_element_match]))

            # check for heading match
            pattern_for_heading = r'\b(?:' + \
                '|'.join(map(re.escape, self.lab_heading_list)) + r')'
            heading_match = re.findall(pattern_for_heading, corpus, re.I)
            header_element_match_list = list(set([x for x in heading_match]))

            # panel dates
            panel_dates = self.get_panel_date(d1)

            # corpus_dates
            corpus_dates = DatetimeExtractor.get_date_time_from_corpus_v2(corpus, [
                'dob'])

            # update the meta data for each page
            meta_data.append((page, list(set(d1['section'])), list(set(d1['sub_section'])), table_element_match_list,
                              header_element_match_list,
                              panel_dates, corpus_dates, self.csv_file))
        return meta_data

    def get_page_meta_data_df(self, meta_data):
        """ 
        args:
            list: meta_data list
        return:
            dataFrame
        """
        data = pd.DataFrame(meta_data)
        data.columns = ['page', 'section', 'sub_section', 'table_element_match_list',
                        'header_match_list', 'panel_date', 'corpus_date', 'file_name']
        return data

    def check_section(self, sec_list, sub_sec_ls):
        """ 
        args: 
            list (detected section list)
            list (detected subsection list)
        return:
            bool: (True/False)
        """
        sec = False
        sub_sec = False
        for section in sec_list:
            if str(section) in self.master_section_list and section not in self.master_exclusion_section_list:
                sec = True
            else:
                sec = False
        s1 = set(self.master_subsection_list)
        s2_updated = []
        for sub_sec_ in sub_sec_ls:
            matcher = process.extractOne(
                str(sub_sec_).lower().strip(), self.master_subsection_list, scorer=fuzz.token_sort_ratio)
            if matcher[1] >= 72:
                s2_updated.append(str(matcher[0]))
        s2_updated_set = set(s2_updated)
        common_intersection = s1.intersection(s2_updated_set)
        if len(common_intersection) > 0:
            sub_sec = True
        else:
            sub_sec = False

        return sec and sub_sec

    def check_lab_str(self, lab_test):
        """ 
        The Scope of this Function is sputum spacific (for table page detection)

        """
        cnt = 0
        lab_ls = ['culture', 'gram stain', 'sputum']
        for lab_test_ in lab_test:
            if lab_test_ in lab_ls:
                cnt += 1
        return True if cnt > 0 else False

    def table_checker(self, df):
        """ 
        args:
            dataFrame: meta_data dataframe (to apply custom logic to assign is_table column)
        return:
            dataFrame: meta_data dataframe (with is_table column)
        """
        is_table = []
        for idx, row in df.iterrows():
            matched_table_comp = list(set(
                [i.lower() for i in row['header_match_list']]).intersection(self.lab_heading_list))
            if len(matched_table_comp) >= 3 and len(row['table_element_match_list']) >= 1:
                is_table.append(1)
            else:
                is_table.append(0)
        df['is_table'] = is_table
        return_df = df[df['is_table'] == 1]
        return return_df

    def get_file_page(self, df):
        """ 
        args:
            dataFrame: filterd dataframe(From table_checker function)
        return: 
            dict: {'file_name':[page]}
        """
        unique_file = list(set(df['file_name'].to_list()))
        file_name_and_page_list = {}
        for file in unique_file:
            df_ = df[df['file_name'] == file]
            file_name_and_page_list[file] = []
            for i, row in df_.iterrows():
                file_name_and_page_list[file].append(row['page'])
        return file_name_and_page_list

    def get_page_list(self, df):
        """  
        if df has only one file, call this function to get the list of page number
    
        """
        return df['page'].tolist()
