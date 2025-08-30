import pandas as pd
import re
import boto3
from io import StringIO
# from . constant import date_tags, suppress_date_tag

try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from utility.date_suppression import DateSuppression
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from utility.suppress_handwritten_text import suppress_handwritten_text
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..utility.date_suppression import DateSuppression
    from ..constant.aws_config import aws_access_key_id, aws_secret_access_key
    from ..utility.suppress_handwritten_text import suppress_handwritten_text

class VitalExcerpt:
    def __init__(self, bucket_name, sec_sub_csv_file, date_tags, suppress_date_tag, vital_regex_list, min_max_date, textract_csv):
        self.bucket_name = bucket_name
        self.sec_sub_csv_file = sec_sub_csv_file
        self.date_tags = date_tags
        self.suppress_date_tag = suppress_date_tag
        self.vital_regex_list = vital_regex_list
        self.min_max_date = min_max_date
        self.textract_csv = textract_csv
        try:   
            self.s3_c = boto3.client('s3', region_name='us-east-1')
            self.s3_c.list_buckets()
            print("S3 client initialized successfully using IAM role in vital_excerpts.py.")
        except Exception as e:
            print(f"Failed to initialize S3 client with IAM role: {str(e)} in vital_excerpts.py.")
            if aws_access_key_id and aws_secret_access_key:
                self.s3_c = boto3.client('s3', 
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)
                print("S3 client initialized successfully using manual keys in vital_excerpts.py.")
            else:
                raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in vital_excerpts.py.")

                   
    def get_corpus(self, d1):
        """  
        args:
            self: VitalExcerpt Object
            d1: pandas Dataframe
        return:
            corpus: str
        """
        corpus = ' '.join(d1['Text'].apply(lambda x: str(x)))
        return corpus

    def get_lookup_table_df(self, df):
        """ 
        function helps to trace back, for detected word to get its childId and boundingBox.
        """
        df['text_len'] = df['Text'].str.len()
        df['Start'] = df['text_len'].shift(
            fill_value=0).cumsum() + df.apply(lambda row: row.name, axis=1)
        df['End'] = df['Start']+df['text_len']
        df.drop('text_len',  axis=1, inplace=True)
        return df

    def supress_date(self, date_tags_list):
        """  
        function helps to suppress unwanted dates using their tags (e.g. Printed on)
        """
        date_tags_true = []
        suppress_date_ls = list(map(lambda x: x.lower(), self.suppress_date_tag))
        for i in date_tags_list:
            if str(i[2]).lower() not in suppress_date_ls:
                date_tags_true.append(i)
        return date_tags_true

    def filter_date(self,dt):
        """  
        sometime date can be detected like '1952', to suppress this type of result use this function.
        """
        filter_date = []
        for dates in dt:
            year_check = DatetimeExtractor.validate_date(dates[0], range_dict={'day': (
                1, 31), 'month': (1, 12), 'year': (2015 , 2030), 'year_': (1, 99)})
            if year_check and year_check[0][1] and len(dates[0]) >= 4 and ('/' in dates[0] or '-' in dates[0]):
                filter_date.append(dates)
        return filter_date

    def get_vital_date(self, lookup_table, index, range1, range2):
        """ 
        function helps to extract the date, based on identified nearby section/section end.
        """
        vital_corpus = self.get_corpus(lookup_table[index+range1:index+range2])
        # vital_date = self.supress_date(
        #     DatetimeExtractor.get_date_time_from_corpus_v2(vital_corpus, self.date_tags))
        vital_date = DatetimeExtractor.get_date_time_from_corpus_v2(vital_corpus, self.date_tags)
        vital_date_ = DateSuppression.date_suppression(
            vital_date, self.min_max_date)
        
        filter_date_list = self.filter_date(vital_date_)
        if len(filter_date_list) > 0 and range1 < 0:
            return filter_date_list[-1]
        elif len(filter_date_list) > 0 and range1 >= 0:
            return filter_date_list[0]
        else:
            return []

    def get_vitals(self,  lookup_index=100):
        """ 
        main function to extract the vitals from document using pre-build regex.
        """
        # df_col = ['Page', 'Text', 'SECTION_ENTITY',
        #           'sub_section', 'section', 'Updated_main_section']
        textract_col = ['Geometry.BoundingBox.Width', 'Geometry.BoundingBox.Height',
                        'Geometry.BoundingBox.Left', 'Geometry.BoundingBox.Top', 'Id','TextType']
        df_col = ['Relationships','Page', 'Text', 'section_entity',
                  'sub_section', 'section', 'updated_main_section', 'Geometry.BoundingBox.Top', 
                  'Geometry.BoundingBox.Left', 'Geometry.BoundingBox.Height', 'Geometry.BoundingBox.Width']
        # Load section subsection csv
        sec_subsection_csv_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=self.sec_sub_csv_file)
        sec_subsection_body = sec_subsection_csv_obj['Body']
        sec_subsection_string = sec_subsection_body.read().decode('utf-8')
        df = pd.read_csv(StringIO(sec_subsection_string),
                         na_filter=False, usecols=df_col)
        textract_csv_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=self.textract_csv)
        textract_body = textract_csv_obj['Body']
        textract_string = textract_body.read().decode('utf-8')
        textract_df = pd.read_csv(StringIO(textract_string),
                                  na_filter=False, usecols=textract_col)
        df = suppress_handwritten_text(df, textract_df)
        # df = pd.read_csv(self.sec_sub_csv_file,
        #                  na_filter=False, usecols=df_col)
        file_name = self.sec_sub_csv_file.split("/")[-1].replace("_section_subsection.csv","")
        lookup_table = self.get_lookup_table_df(df)
        lookup_section_start = lookup_table[lookup_table['section_entity'] == "SECTION"].copy(
        )
        lookup_section_end = lookup_table[lookup_table['section_entity'] == "SECTION END"].copy(
        )
        meta_data = []

        # corpus for excerpts
        corpus = self.get_corpus(df)
        date_from_flag = 0 # always 0 for excerpt

        # iterate through each vital regex, and apply it to corpus.
        for vit, reg in self.vital_regex_list:
            match_obj = re.finditer(reg, corpus, re.I)
            for each_match in match_obj:
                meta_data_df = lookup_table[(lookup_table['Start'] <= each_match.start())
                                            & (lookup_table['End'] >= each_match.end())]
                try:
                    start_page = lookup_table[lookup_table['Start'] <= each_match.start()].tail(1)[
                        'Page'].values[0]
                    end_page = lookup_table[lookup_table['End'] >= each_match.end()].head(1)[
                        'Page'].values[0]
                    if start_page == end_page:
                        try:
                            index = lookup_table[(lookup_table['Start'] <= each_match.start())
                                                 & (lookup_table['End'] >= each_match.end())].index.values[0]
                        except:
                            index = lookup_table[lookup_table['Start'] <= each_match.start()].tail(
                                1).index.values[0]
                            flag = 1
                        # bring the BoundingBox information of components
                        top = lookup_table.iloc[index]['Geometry.BoundingBox.Top']
                        left = lookup_table.iloc[index]['Geometry.BoundingBox.Left']
                        height = lookup_table.iloc[index]['Geometry.BoundingBox.Height']
                        width = lookup_table.iloc[index]['Geometry.BoundingBox.Width']

                        # Above the corpus (look for date)
                        vital_date = self.get_vital_date(
                            # lookup_table, index, 1, 11)
                            lookup_table, index, -10, 1)

                        # Down the corpus (look for date)
                        if len(vital_date) == 0:
                            vital_date = self.get_vital_date(
                                # lookup_table, index, -10, 1)
                                lookup_table, index, 1, 11)

                        # Section Start (look for date)
                        if len(vital_date) == 0:
                            lookup_section_start_index = lookup_section_start[lookup_section_start['Start'] <= each_match.start(
                            )].tail(1).index.values[0]
                            vital_date = self.get_vital_date(
                                lookup_table, lookup_section_start_index, 0, 5)

                        # Section End (look for date)
                        if len(vital_date) == 0:
                            lookup_section_end_index = lookup_section_end[lookup_section_end['End'] >= each_match.end(
                            )].head(1).index.values[0]
                            vital_date = self.get_vital_date(
                                lookup_table, lookup_section_end_index, -5, 5)

                        vital_val = re.findall(reg, each_match.group(), re.I)
                        vital_val_ = vital_val[0] if vital_val else None
                
                        # section = list(set(df[df['Page'] == start_page]
                        #      ['section'].to_list()))
                        # sub_section = list(set(df[df['Page'] == start_page]
                        #      ['sub_section'].to_list()))
                        
                        section = str(df.iloc[index]['section']) if str(df.iloc[index]['section']).lower() != 'nan' else ''
                        sub_section = str(df.iloc[index]['sub_section']) if str(df.iloc[index]['sub_section']).lower() != 'nan' else ''
                        main_section = str(df.iloc[index]['updated_main_section']) if str(
                            df.iloc[index]['updated_main_section']).lower() != 'nan' else ''
                        # preprocess date
                        dt, tm, _ = vital_date if vital_date else (None, None, None)
                        vital_processed_date = f"{dt if dt else ''} {tm if tm else ''}"
                        bb_info = list([top, left, height, width])
                        is_from = 'excerpt'
                        meta_data.append(
                            # (vital_processed_date, vit, vit, vital_val_, start_page, section, sub_section, file_name))
                            (file_name, start_page, vit, vital_val_, vital_processed_date, section, sub_section, main_section, date_from_flag, bb_info, is_from))
                    else:
                        pass

                except:
                    pass
        return meta_data
    def save_vital_result(self, meta_data, path_to_save):
        """ 
        function helps to save the extracted result.
        """
        if len(meta_data)> 0:
            df = pd.DataFrame(meta_data)
            df.columns = ['DocumentName', 'Page', 'TestName', 'TestResult', 'TestDateTime', 'Section', 'SubSection', 'MainSection',
                          'DateFrom', 'BBInfo', 'IsFrom']
            df['TestUnit'] = ''
            # df.columns = ['page', 'vital', 'value', 'date']
            # df.columns = ['date', 'type', 'name',
            #               'text', 'page_no', 'section', 'sub_section', 'file_name']
            
            csv_buf = StringIO()
            df.to_csv(csv_buf, header=True, index=False)
            self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
            ), Key=f"{path_to_save}/vital_excerpt/{self.sec_sub_csv_file.split('/')[-1].replace('_section_subsection.csv', '')}_vitals.csv")
            # self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
            # ), Key=f'{path_to_save}/{self.sec_sub_csv_file.split("_")[-1].replace(".csv", "")}_vitals.csv')
            csv_buf.seek(0)
            # df.to_csv(
            #     rf"{path_to_save}/{self.sec_sub_csv_file.split('_')[-1].replace('.csv', '')}_vitals.csv")
            return df
        else:
            return pd.DataFrame({})


