import pandas as pd
import numpy as np
import re
import ast
import boto3
import json
from dateutil.parser import parse
from datetime import datetime, timedelta
from io import StringIO
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from utility.suppress_handwritten_text import suppress_handwritten_text
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..constant.aws_config import aws_access_key_id, aws_secret_access_key
    from ..utility.suppress_handwritten_text import suppress_handwritten_text


class ExcerptExtraction:
    def __init__(self, sec_sub_csv_file, textract_csv,bucket_name, date_tags, suppress_date_tag,min_max_date, template_name, attribute_dict, attribute_regex_dict):
        self.sec_sub_csv_file = sec_sub_csv_file
        self.textract_csv = textract_csv
        self.bucket_name = bucket_name
        self.date_tags = date_tags
        self.suppress_date_tag = suppress_date_tag
        self.min_max_date = min_max_date
        self.template_name = template_name
        self.attribute_dict = attribute_dict
        self.attribute_regex_dict = attribute_regex_dict
        # Set the name of the SageMaker endpoint to invoke
        self.endpoint_name = 'huggingface-pytorch-inference-2024-03-11-11-42-09-280'
        try:
            self.client = boto3.client('sagemaker-runtime', region_name='us-east-1')
            input_data = {
                    'inputs': 'The patient recovered during the night and now denies any [entity] shortness of breath [entity].'
                }
            payload = json.dumps(input_data)
            response = self.client.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType='application/json',
            Body=payload
            )
            print('SageMaker client initialized successfully with default credentials.')
        except Exception as e:
            print('Exception :',e)
            self.client = boto3.client('sagemaker-runtime', 
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    region_name='us-east-1')
            
            response = self.client.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType='application/json',
            Body=payload
            )
            print('SageMaker client initialized successfully with provided credentials.')
        
    def get_corpus(self, d1):
        corpus = ' '.join(d1['Text'].apply(lambda x: str(x)))
        return corpus

    # excerpt clean-up //added
    def clean_excerpt(self, excerpt_str):
        excerpt_str = str(excerpt_str)
        match = re.findall(
            r'\s([^A-za-z0-9]?[a-zA-Z].*(?<=(?:\s)))', excerpt_str)
        if match:
            # return match[0]
            return " ".join(str(match[0]).split(' ')[:-1])
        else:
            return excerpt_str
    def add_entity_token(self, excerpt,match_group):

        modified_text = re.sub(match_group, r"[entity] \g<0> [entity]", excerpt)
        return modified_text

    # excerpt negation:custom //added
    def check_negation(self, excerpt_str, idx):
        negation_list = ['Not',
                         'No',
                         'Negative',
                         'Absent',
                         'Denies',
                         'Unremarkable',
                         'None',
                         'Normal',
                         'Without',
                         'Free of',
                         'No evidence',
                         'Rule out',
                         'Excluded',
                         'Declined',
                         'Refused',
                         'Unaffected',
                         'Not present',
                         'Unlikely',
                         'No Trace',
                         'No clear',
                         'insignificant',
                         'no indication',
                         'no signs',
                         'no symptoms',
                         'Unaffected',
                         'Inconclusive',
                         'Insignificant',
                         'Inapplicable',
                         'Unconfirmed',
                         'Unchanged',
                         'Unaltered',
                         'Unaffected',
                         'Disproved',
                         'Unassociated',
                         'Unconfirmed',
                         'Unchanged',
                         'Unremarked',
                         'Irrelevant',
                         'Uninvolved',
                         'Unrelated',
                         'Not seen',
                         'Not detected',
                         'Noted absence of',
                         'absence of',
                         'not evident',
                         'not showing',
                         'no signs',
                         'neither',
                         'nor',
                         'not seen',]
        pattern_for_negation = r'\b(?:' + \
            '|'.join(map(re.escape, negation_list)) + r')\b'
        
        context = excerpt_str[idx-30:idx+30]
        match = re.findall(pattern_for_negation, context, re.I)
        if match:
            return 1
        else:
            return 0

        # excerpt negation:model //added
    def check_negation_m1(self, data):
        def get_neagation_flag(label):
            if label == 'ABSENT':
                return 1
            else:
                return 0
    
        # endpoint_name = 'huggingface-pytorch-inference-2024-03-11-11-42-09-280'
        # Set the content type of the input data
        content_type = 'application/json'

        # Set the input data
        input_data = {
            'inputs': str(data)
        }

        # Convert the input data to JSON format
        payload = json.dumps(input_data)

        # Invoke the SageMaker endpoint
        response = self.client.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType=content_type,
            Body=payload
        )

        # Get the response from the SageMaker endpoint
        result = response['Body'].read().decode('utf-8')
        flag = get_neagation_flag(eval(result)[0].get('label'))
        return flag

    # def check_negation_m_local(self, excerpt_str, idx):
    #     def get_neagation_flag(label):
    #         if label == 'ABSENT':
    #             return 1
    #         else:
    #             return 0
    #     input = str(excerpt_str[idx-30:idx+30])
    #     classification = classifier(input)
    #     flag = get_neagation_flag(classification[0].get('label'))
    #     return flag

    def get_lookup_table_df(self, df):
        df['text_len'] = df['Text'].str.len()
        df['Start'] = df['text_len'].shift(
            fill_value=0).cumsum() + df.apply(lambda row: row.name, axis=1)
        df['End'] = df['Start']+df['text_len']
        df.drop('text_len',  axis=1, inplace=True)
        return df

    def get_metadata_row(self, lookup_table, match_group, start_, end_):
        """ 
        args:
            match_group: str
            start_: int
            end_: int
        return:
            (page, section, sub_section, child_id): tuple
        """
        meta_data_df = lookup_table[(lookup_table['Start'] <= start_)
                                    & (lookup_table['End'] >= end_)]

        page, child_id, section, sub_section, main_section = None, [], '', '', ''
        # for matched_word in match_group.split():
        # for idx, word in enumerate(meta_data_df['Text'].values[0].split()):

        for matched_word in re.split(r"[^A-Za-z0-9]", match_group):
            try:
                for idx, word in enumerate(meta_data_df['Text'].values[0].split()):
                    if matched_word in re.split(r"[^A-Za-z0-9]", word):
                        try:
                            child_id.append(ast.literal_eval(
                                meta_data_df['Relationships'].values[0])[0]['Ids'][idx])
                        except IndexError as e:
                            pass
            except IndexError as e:
                pass
        try:
            page = meta_data_df["Page"].values[0]
            # section = list(set(lookup_table[lookup_table['Page'] == page]
            #                    ['section'].to_list()).difference({np.nan}))
            # sub_section = list(set(lookup_table[lookup_table['Page'] == page]
            #                        ['sub_section'].to_list()).difference({np.nan}))
            section = meta_data_df["section"].values[0] if str(meta_data_df["section"].values[0]).lower() != 'nan' else ''
            sub_section = meta_data_df["sub_section"].values[0] if str(
                meta_data_df["sub_section"].values[0]).lower() != 'nan' else '' 
            main_section = meta_data_df["updated_main_section"].values[0] if str(
                meta_data_df["updated_main_section"].values[0]).lower() != 'nan' else ''
        except IndexError as e:
            pass
        return (page, section, sub_section,main_section, child_id)

    def get_bounding_box(self, df, child_id):
        df_ = df[df['Id'] == child_id]
        width = df_['Geometry.BoundingBox.Width'].values[0]
        height = df_['Geometry.BoundingBox.Height'].values[0]
        top = df_['Geometry.BoundingBox.Top'].values[0]
        left = df_['Geometry.BoundingBox.Left'].values[0]
        return (width, height, top, left)

    def get_regex_pattern_list(self, term_list, flag):
        def isfloat(str):
            try:
                flag = float(str)
                return True if flag else False
            except ValueError as e:
                return False
            
        word_for_regex = []
        special_char_list, non_special_char_list = [], []
        for ele in term_list:
            match_ = re.findall(r'[^A-Za-z0-9.]', ele)
            if match_:
                mather_len = max([len(i.strip()) for i in match_])
                special_char_list.append(
                    ele) if mather_len >= 1 else non_special_char_list.append(ele)
            else:
                non_special_char_list.append(ele)

        for term in non_special_char_list:
            word_1 = []
            word_2 = []
            for word in term.split():
                word = word.replace("(", '').replace(")", '')
                try:
                    if word.isupper() and flag:
                        word_1.append(word)
                        if len(term.split()) > 1:
                            word_2.append(word)
                    elif word.isnumeric() or isfloat(word):
                        word_1.append(word)
                        word_2.append(word)
                    else:
                        if (word[0].isupper() or word[0].islower()) and word[0].isalpha():
                            word_1.append(
                                f"[{word[0].lower()}{word[0].upper()}]{word[1:].lower()}")
                            if not (word[1:].islower() or word[1:].isupper()):
                                word_2.append(word)
                            else:
                                word_2.append(
                                    f"{word.upper()}")
                        else:
                            idx_ = next(re.finditer(r'[A-Za-z]', word)).start()
                            word_1.append(
                                f"{word[:idx_]}[{word[idx_].upper()}{word[idx_]}]{word[idx_+1:].lower()}")
                            word_2.append(f"{word.upper()}")
                except IndexError as e:
                    pass
            word_for_regex.append(" ".join(word_1))
            word_for_regex.append(" ".join(word_2))

        # preprocess the list (as its containe '', duplicate)
        word_for_regex.append(" ".join(special_char_list))
        word_for_regex+=special_char_list
        word_for_regex_ = [word for word in word_for_regex if len(word) > 1]
        word_for_regex_ls = list(set(word_for_regex_))
        return word_for_regex_ls

    def date_parser(self, date_str, default_date=(2020, 1, 1)):
        """
        date_parser function used to structure the date format (mm-dd-yyyy hh:mm:ss)
        args:
            str: date_string
            tuple: default_date
        return:
            str: date_str (formatted)
        """
        yy, mm, dd = default_date
        default_date_ = datetime(yy, mm, dd)

        date_str = date_str.replace('-', '/')

        date_str_obj = DatetimeExtractor.get_date_time_from_corpus_v2(date_str, [
                                                                    'None'])
        if date_str_obj and date_str_obj[0][1] and len(date_str_obj[0][1]) <= 4 and ':' not in date_str_obj[0][1]:
            # add extra 00 at the end of time
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+'00'}"
        elif date_str_obj and not date_str_obj[0][1]:
            date_str = f"{date_str_obj[0][0]} 00:00:00"
        elif date_str_obj and len(date_str_obj[0][1]) >= 5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1][:8]}"
        elif date_str_obj and len(date_str_obj[0][1]) <= 5:
            date_str = f"{date_str_obj[0][0]} {date_str_obj[0][1]+':00'}"
        else:
            date_str = None
        # parse the date
        if date_str:
            try:
                parsed_date = parse(date_str, default=default_date_)
                day = parsed_date.day
                month = parsed_date.month
                year = parsed_date.year

                hour = parsed_date.hour
                minute = parsed_date.minute
                seconds = parsed_date.second
                if day < 10:
                    day_ = f'0{day}'
                else:
                    day_ = day
                return f"{month}-{day_}-{year} {hour}:{minute}:{seconds}"
            except Exception as e:
                pass
        else:
            return None
    def guard_rail_date(self, inp_date, min_max_date, yr):
        inp_date = self.date_parser(inp_date, (yr, 1, 1))
        min_date, max_date = min_max_date
        flag = False
        try:
            flag = parse(min_date)-timedelta(3) <= parse(inp_date) <= parse(max_date)
        # flag_ = True
        # for res_date in restricted_suppress_date_ls:
        #     res_date_ = f"{res_date[0]} {'' if not res_date[1] else res_date[1]}"
        #     if inp_date == self.date_parser(res_date_, (yr, 1, 1)):
        #         flag_ = False  # add break here
        #         break
        except TypeError as e:
            pass
        if flag == True :
            return True
        else:
            return False
        
    def get_date(self, corpus, date_, match):
        min_date, max_date = self.min_max_date
        try:
            min_year, max_year = parse(min_date).year, parse(max_date).year
        except ValueError as e:
            min_year = 2000
            max_year = 2030
        restrict_unwanted_date = {'day': (1, 31), 'month': (
            1, 12), 'year': (min_year, max_year), 'year_': (1, 99)}

        increment_idx = 250
        date_ls = []
        while not date_ls and increment_idx <= 430 and not date_:
            detected_date_ls = DatetimeExtractor.get_date_time_from_corpus_v2(
                corpus[match.start()-increment_idx:match.end()+25], self.date_tags, restrict_dict=restrict_unwanted_date)
            if len(detected_date_ls) > 0:
                for dt in detected_date_ls:
                    date_obj = dt[0]
                    time_obj = '' if not dt[1] else dt[1]
                    tag_obj = dt[2]
                    date_time_obj_str = f'{date_obj} {time_obj}'
                    if tag_obj not in self.suppress_date_tag and self.guard_rail_date(date_time_obj_str, self.min_max_date, min_year
                                                                                      ):
                        date_ls.append(
                            date_time_obj_str)
                        break
            increment_idx += 60
        date_ = date_ls[0] if date_ls else ''
            
        return date_

    def get_value_from_excerpt(self, attr, excerpt):
        """ 
        function used to extract the result value using regex.
        args:
            attr: str (regex string)
            excerpt: str 
        return:
            result_value: str
        """
        attr_regex = self.attribute_regex_dict.get(attr)
        test_result = None
        test_unit = None
        if attr_regex:
            result_value = re.findall(attr_regex, excerpt, re.I)
            # return result_value[0] if result_value else None
            if result_value:
                # try:
                #     test_result, test_unit = result_value[0]
                #     return test_result, test_unit
                # except ValueError as e:
                #     test_result = result_value[0]
                #     return test_result, test_unit
                if isinstance(result_value[0], tuple):
                    test_result, test_unit = result_value[0]
                    return test_result, test_unit
                else:
                    test_result = result_value[0]
                    return test_result, test_unit
            else:
                return test_result, test_unit
        else:
            return None, None

    def get_excerpts(self, lookup_index=100):
        """ 
        args:
            lookup_index: int
        return:
            meta_data: list [(attribute_name, page, section, sub_section, excerpt_list, file_name)]
        """
        df_col = ['Relationships',
                  'Page', 'section', 'sub_section', 'Text', 'post_process_date/time', 'section_entity', 'updated_main_section']
        # df_col = ['Relationships',
        #           'Page', 'section', 'sub_section', 'Text', 'Post Process Date/Time', 'SECTION_ENTITY', 'Updated_main_section']
        textract_col = ['Geometry.BoundingBox.Width', 'Geometry.BoundingBox.Height',
                        'Geometry.BoundingBox.Left', 'Geometry.BoundingBox.Top', 'Id', 'TextType']
        
        # Load section subsection csv
        try:   
            self.s3_c = boto3.client('s3', region_name='us-east-1')
            self.s3_c.list_buckets()
            print("S3 client initialized successfully using IAM role in excerpt_extraction.py.")
        except Exception as e:
            print(f"Failed to initialize S3 client with IAM role: {str(e)} in excerpt_extraction.py.")
            if aws_access_key_id and aws_secret_access_key:
                self.s3_c = boto3.client('s3', 
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)
                print("S3 client initialized successfully using manual keys in excerpt_extraction.py.")
            else:
                raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in excerpt_extraction.py.")

        sec_subsection_csv_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=self.sec_sub_csv_file)
        sec_subsection_body = sec_subsection_csv_obj['Body']
        sec_subsection_string = sec_subsection_body.read().decode('utf-8')
        df = pd.read_csv(StringIO(sec_subsection_string),
                         na_filter=False, usecols=df_col)
        # df = pd.read_csv(self.sec_sub_csv_file,
        #                  na_filter=False, usecols=df_col)
        textract_csv_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=self.textract_csv)
        textract_body = textract_csv_obj['Body']
        textract_string = textract_body.read().decode('utf-8')
        textract_df = pd.read_csv(StringIO(textract_string),
                                  na_filter=False, usecols=textract_col)
        df = suppress_handwritten_text(df, textract_df)
        # get unwanted/restricted date
        # corpus = self.get_corpus(df)
        # restrict_date_ls = DatetimeExtractor.get_date_time_from_corpus_v2(
        #     corpus, self.date_tags)
        # restricted_suppress_date_ls_ = [
        #     i for i in restrict_date_ls if i[-1] and i[-1] in self.suppress_date_tag]
        # ----------------------
        min_date, max_date = self.min_max_date
        try:
            min_year, max_year = parse(min_date).year, parse(max_date).year
        except ValueError as e:
            min_year = 2000
            max_year = 2030
        # ----------------------------

        # self.restricted_suppress_date_ls = list(
        #     set(restricted_suppress_date_ls_))

        lookup_table = self.get_lookup_table_df(df)
        meta_data = []
        for attribute_name, att in self.attribute_dict.items():

            # corpus for excerpts
            if att.get('generic_term'):
                # true/false
                flag_generic = any(
                    map(lambda x: x.isupper(), att.get('generic_term')))

                word_for_regex = self.get_regex_pattern_list(
                    att.get('generic_term'), flag_generic)
                pattern_for_generic_term = r'\b(?:' + \
                    '|'.join(word_for_regex) + r')\b'

            flag_specific = any(
                map(lambda x: x.isupper(), att.get('specific_term')))
            word_for_regex_specific = self.get_regex_pattern_list(
                att.get('specific_term'), flag_specific)

            pattern_for_spacific_term = r'\b(?:' + \
                '|'.join(word_for_regex_specific) + r')\b'

            corpus = self.get_corpus(df)

            # page, sec, sub_sec, excerpts_list, generic_matched_term, generic_bb, excerpt_matched_term, excerpt_bb = [], [], [], [], [],[],[],[]

            if att.get('generic_term'):
                generic_term_match = re.finditer(
                    pattern_for_generic_term, corpus,) if flag_generic else re.finditer(
                    pattern_for_generic_term, corpus, re.I)
                prev_end = None
                prev_matching_group = None

                for match_ in generic_term_match:

                    page, sec, sub_sec,main_section, date_, excerpts, generic_matched_term, generic_bb, excerpt_matched_term, excerpt_bb = None, None,None, None, None, None, None, [], [], []
                    sub_corpus_start_idx = match_.start()-int(lookup_index * 2.5)
                    sub_corpus_end_idx = match_.start()+int(lookup_index * 2.5)
                    spacific_term_matcher = re.finditer(pattern_for_spacific_term, corpus[sub_corpus_start_idx:
                                                                                          sub_corpus_end_idx],) if flag_specific else re.finditer(pattern_for_spacific_term, corpus[sub_corpus_start_idx:
                                                                                                                                                                                    sub_corpus_end_idx], re.I)
                    spacific_matched_term_ = [
                        (j.group(), j.start(), j.end()) for j in spacific_term_matcher]

                    if spacific_matched_term_:
                        ########  generic term #############
                        generic_matched_term = match_.group()

                        page_no_generic, sec, sub_sec, main_section, generic_child_id = self.get_metadata_row(lookup_table,
                                                                                                match_.group(),
                                                                                                match_.start(),
                                                                                                match_.end())
                        page_no_generic = int(page_no_generic) if isinstance(
                            page_no_generic, (int, float)) else page_no_generic
                        generic_bb.append([self.get_bounding_box(
                            textract_df, child_id) for child_id in generic_child_id])

                        ########  specific term #############
                        detected_spacific_term_pages = []
                        for excp_term in spacific_matched_term_:
                            excerpt_start_idx, excerpt_end_idx = excp_term[1] + \
                                sub_corpus_start_idx, excp_term[2] + \
                                sub_corpus_start_idx
                            page_no_spacific, _, _,_, excerpt_child_id = self.get_metadata_row(lookup_table,
                                                                                             excp_term[0],
                                                                                             excerpt_start_idx,
                                                                                             excerpt_end_idx)
                            page_no_spacific = int(page_no_spacific) if isinstance(
                                page_no_spacific, (int, float)) else page_no_spacific
                            # check if both generic and spacific term are on the same page then only update the result(meta data)
                            if page_no_generic == page_no_spacific:
                                excerpt_matched_term.append(excp_term[0])
                                excerpt_bb.append([self.get_bounding_box(
                                    textract_df, child_id) for child_id in excerpt_child_id])
                                detected_spacific_term_pages.append(
                                    page_no_spacific)

                        # update list (both spacific and generic has same page)
                        if generic_matched_term and len(excerpt_matched_term) >= 1:
                            # if not set(detected_spacific_term_pages).difference([page_no_generic]):
                            excerpts = corpus[match_.start(
                            )-lookup_index:match_.end()+lookup_index]
                    
                        date_ = self.get_date(corpus, date_, match_)
                    

                        if not date_:
                            # section
                            try:
                              
                                date_ = df[(match_.start() >= df['Start']) & (df['section_entity'].isin(
                                    ['SECTION']))].iloc[-1]['post_process_date/time']
                            except IndexError as e:
                                date_ = ''
                                pass
                            # section end
                            try:
                                date_ = df[(match_.end() <= df['End']) & (df['section_entity'].isin(
                                    ['SECTION END']))].iloc[0]['post_process_date/time']
                            except IndexError as e:
                                date_ = ''
                                pass
                        # last sanity check for date
                        if not (date_ and  self.guard_rail_date(date_, self.min_max_date, min_year)):
                            date_ = ''
                      
                        
                    # if excerpts and prev_end and (prev_end <= match_.start() <= prev_end+50) and (prev_matching_group == match_.group()):
                    #     print(
                    #         f"suppress: {attribute_name} | {match_.group()} ")
                    if excerpts:
                        test_result, test_unit = self.get_value_from_excerpt(
                            attribute_name, excerpts)
                        csv_f = self.sec_sub_csv_file.split(
                            "_")[-1].replace('.csv', '')
                        
                        # isNegated = self.check_negation(excerpts, match_.start())
                        idx = excerpts.index(match_.group())
                        
                        data = str(excerpts[idx-30:idx+30])
                        data = self.add_entity_token(data, match_.group())

                        isNegated = self.check_negation_m1(
                            data)
                        cleaned_excerpt = self.clean_excerpt(excerpts)
                        isFrom = 'excerpt'
                        meta_data.append((attribute_name, page_no_generic, sec, sub_sec, main_section, cleaned_excerpt, date_, test_result, test_unit,
                                          generic_matched_term, generic_bb, excerpt_matched_term, excerpt_bb, att.get('color'), str(csv_f), isNegated, isFrom))
            else:
                excerpt_matcher = re.finditer(
                    pattern_for_spacific_term, corpus,) if flag_specific else re.finditer(
                    pattern_for_spacific_term, corpus, re.I)
                
                prev_end = None
                prev_matching_group = None
                for match_ in excerpt_matcher:
                    page, sec, sub_sec,main_section, date_, excerpts, generic_matched_term, generic_bb, excerpt_matched_term, excerpt_bb = None,None, None, None, None, None, None, [], [], []

                    excerpt_matched_term = match_.group()
                    page, sec, sub_sec,main_section, generic_child_id = self.get_metadata_row(lookup_table,
                                                                                 match_.group(),
                                                                                 match_.start(),
                                                                                 match_.end())
                    page = int(page) if isinstance(
                        page, (int, float)) else page
                    excerpts = corpus[match_.start(
                    )-lookup_index:match_.end()+lookup_index]
                    excerpt_bb.append([self.get_bounding_box(
                        textract_df, child_id) for child_id in generic_child_id])

                
                    date_ = self.get_date(corpus, date_, match_)
                

                    if not date_:
                        # section
                        try:
                        
                            date_ =  df[(match_.start() >= df['Start']) & (df['section_entity'].isin(
                                ['SECTION']))].iloc[-1]['post_process_date/time']
                        except IndexError as e:
                            date_ = ''
                        # section end
                        try:
                            date_ = df[(match_.end() <= df['End']) & (df['section_entity'].isin(
                                ['SECTION END']))].iloc[0]['post_process_date/time']
                        except IndexError as e:
                            date_ = ''
                    # last sanity check for date
                    if not (date_ and self.guard_rail_date(date_, self.min_max_date, min_year)):
                        date_ = ''
                   

                    # if excerpts and prev_end and (prev_end <= match_.start() <= prev_end+50) and (prev_matching_group == match_.group()):
                    #     print(
                    #         f"suppress: {attribute_name} | {match_.group()} ")
                    if excerpts:

                        test_result, test_unit = self.get_value_from_excerpt(
                            attribute_name, excerpts)
                        csv_f = self.sec_sub_csv_file.split(
                            "_")[-1].replace('.csv', '')
                        idx = excerpts.index(match_.group())
                        
                        data = str(excerpts[idx-30:idx+30])
                        data = self.add_entity_token(data, match_.group())
                        
                        isNegated = self.check_negation_m1(
                            data)
                        cleaned_excerpt = self.clean_excerpt(excerpts)
                        isFrom = 'excerpt'
                        meta_data.append((attribute_name, page, sec, sub_sec, main_section, cleaned_excerpt, date_, test_result, test_unit,
                                          generic_matched_term, generic_bb, excerpt_matched_term, excerpt_bb, att.get('color'), str(csv_f), isNegated, isFrom))

        return meta_data
    
    def check_exclusion1(self,row):
            excluded_data1 = []
            attribute_name = row['TestName']
            text = row['Text']
            exclusion_terms = self.attribute_dict.get(attribute_name, {}).get("exclusion_term", [])
            for term in exclusion_terms:
                term = term.lower()
                if term in text.lower():
                    excluded_data1.append({
                        "attribute_name": attribute_name,
                        "excluded_term": term,
                        "excerpts": text
                    })
                    
                    return True
                
            return False
    def check_exclusion(self,df):
        df['exclusion_result']= df.apply(lambda row: self.check_exclusion1(row), axis=1)
        df_filtered = df.drop(df[df['exclusion_result']].index)
        df_filtered = df_filtered.reset_index(drop=True)
        
        
        return df_filtered

    def get_result_df(self, result_data):
        """ 
        function helps to get the dataFrame from generated excerpt result.
        args:
            self: Pneumonia Object
            result_data: list
        return:
            result_data_df: pandas dataframe
        """
        if result_data:
            result_data_df = pd.DataFrame(result_data)
            result_data_df.columns = ['TestName', 'Page',
                                      'Section', 'SubSection', 'MainSection', 'Text', 'TestDateTime', 'TestResult', 'TestUnit', 'GenericTerm', 'GenericTermBB', 'SpecificTerm', 'SpecificTermBB', 'Color', 'DocumentName', 'isNegated', 'IsFrom']
            result_data_df = result_data_df.sort_values(by=['Page'])
            # result_data_df['page_no'].dropna(inplace=True)
            result_data_df = result_data_df[~result_data_df['Page'].isna()]
            result_data_df['Page'] = result_data_df['Page'].astype(
                'Int32')
            return result_data_df
        else:
            return pd.DataFrame({})

    def save_excerpt_result(self, result_data_df,s3_c,bucket_name, path_to_save):
        if result_data_df.shape[0] >= 1:
            # result_data_df.to_csv(
            #     rf"{path_to_save}\{self.sec_sub_csv_file.split('_')[-1].replace('.csv', '')}_{self.template_name}.csv", index=False)
            csv_buf = StringIO()
            result_data_df.to_csv(csv_buf, header=True, index=False)
            s3_c.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
            ), Key=f"{path_to_save}/template_excerpt/{self.sec_sub_csv_file.split('/')[-1].replace('_section_subsection.csv', '')}_{self.template_name}.csv")
            # s3_c.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
            # ), Key=f"{path_to_save}/{self.sec_sub_csv_file.split('_')[-1].replace('.csv', '')}_{self.template_name}.csv")
            csv_buf.seek(0)
