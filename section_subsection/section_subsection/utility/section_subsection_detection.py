import os
import re
import json
import time
import boto3
import string
import random
import logging
import warnings
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from rapidfuzz import fuzz, process
import requests
warnings.filterwarnings("ignore")

try:
    from postprocess.post_process import Postprocess
    from constants.constant_dict import creds_dict, creds_rank
    from date_time_module.datetime_extractor import DatetimeExtractor
    from constants.aws_config import aws_access_key_id, aws_secret_access_key
    from constants.section_constant_terms import fp_text, section_continue_terms, section_exclude_patterns
    from constants.section_end_constants import creds_pattern, section_end_patterns,suppress_section_end_pattern, section_end_start_pattern, section_end_end_pattern
    from constants.date_tag_constant import adm_dis_tag, date_tags, suppress_date_tag_section, suppress_date_tag_section_end, section_date_tags, section_end_date_tags, date_tags_regex_dict

except ModuleNotFoundError as e:
    from ..postprocess.post_process import Postprocess
    from ..constants.constant_dict import creds_dict, creds_rank
    from ..date_time_module.datetime_extractor import DatetimeExtractor
    from ..constants.aws_config import aws_access_key_id, aws_secret_access_key
    from ..constants.section_constant_terms import fp_text, section_continue_terms, section_exclude_patterns
    from ..constants.section_end_constants import creds_pattern, section_end_patterns,suppress_section_end_pattern, section_end_start_pattern, section_end_end_pattern
    from ..constants.date_tag_constant import adm_dis_tag, date_tags, suppress_date_tag_section, suppress_date_tag_section_end, section_date_tags, section_end_date_tags, date_tags_regex_dict

try:
    from utils import read_csv_file
except ModuleNotFoundError as e:
    from .utils import read_csv_file

class Section:
    def __init__(self, textract_file, bucket_name, file_name, document_name,section_subsection_constant, date_tag_constant, zai_emr_system_name, zai_emr_system_version, grd_file) -> None:
        self.textract_file = textract_file
        self.file_name = file_name
        self.document_name=document_name
        self.bucket_name = bucket_name
        self.section_continue_terms_regex = "|".join(section_subsection_constant['section_constant_terms']['section_continue_terms'])
        self.fp_text = section_subsection_constant['section_constant_terms']['fp_text']
        self.section_exclude_patterns = section_subsection_constant['section_constant_terms']['section_exclude_patterns']
        
        self.creds_pattern = section_subsection_constant['physician_credentials']['creds_pattern']
        self.section_end_patterns = section_subsection_constant['section_end_constant_terms']['section_end_patterns']
        self.suppress_section_end_pattern = section_subsection_constant['section_end_constant_terms']['suppress_section_end_pattern']
        self.section_end_start_pattern = section_subsection_constant['section_end_constant_terms']['section_end_start_pattern']
        self.section_end_end_pattern = section_subsection_constant['section_end_constant_terms']['section_end_end_pattern']

        self.adm_dis_tag = date_tag_constant['adm_dis_tag']
        self.date_tags = date_tag_constant['date_tags']
        self.suppress_date_tag_section = date_tag_constant['suppress_date_tag_section']
        self.suppress_date_tag_section_end = date_tag_constant['suppress_date_tag_section_end']
        self.section_end_date_tags = date_tag_constant['section_end_date_tags']
        self.date_tags_regex_dict = date_tag_constant['date_tags_regex_dict']
        self.section_date_tags = date_tag_constant['section_date_tags']
        
        self.creds_dict = section_subsection_constant['physician_credentials']['creds_dict']
        self.creds_rank = section_subsection_constant['physician_credentials']['creds_rank']
        
        self.s3_c = boto3.client('s3', region_name='us-east-1')
        
        self.parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]
        
        # Load stop words
        stop_words_file_path = os.environ["ZAI_STOP_WORD_PARAMS_PATH"]
        print(f"Reading stop_words_file -'{self.parameters_bucket_name}' - '{stop_words_file_path}'")
        stop_words = read_csv_file(self.s3_c, self.bucket_name, stop_words_file_path, encoding='latin')
        self.stop_words_list = stop_words['Stop_words'].tolist()
        logging.info('Stop words loaded')

        self.zai_emr_system_name = zai_emr_system_name
        self.zai_emr_system_version = zai_emr_system_version
        self.grd_file = grd_file

    def toc_checker(self, df):
        """
        ### Checks for table of contents present in the corpus and gets relavant section subsection list
        args:
            df: DataFrame
        return:
            None
        """
        # load the sections
        zai_distinct_section_params_key = os.environ["ZAI_DISTINCT_SECTION_PARAMS_PATH"]
        print(f"Reading zai_distinct_section_params -'{self.parameters_bucket_name}' - '{zai_distinct_section_params_key}'")
        section_data = read_csv_file(self.s3_c, self.parameters_bucket_name, zai_distinct_section_params_key, encoding='latin')
        logging.info('Pre defined Section Names loaded')

        # Load sub section data
        zai_distinct_sub_section_params_key = os.environ["ZAI_DISTINCT_SUB_SECTION_PARAMS"]
        print(f"Reading zai_distinct_sub_section_params -'{self.parameters_bucket_name}' - '{zai_distinct_sub_section_params_key}'")
        sub_section_data = read_csv_file(self.s3_c, self.parameters_bucket_name, zai_distinct_sub_section_params_key, encoding='latin')
        logging.info('Pre defined Sub Section Names loaded')
        
        col_name = f"{self.zai_emr_system_name}_{self.zai_emr_system_version}"
        section_data = section_data[section_data[col_name] == 1].reset_index(drop=True)
        
        corpus = ' '.join(df['Text'].apply(lambda x: str(x)).to_list())
        table_of_content_match = re.findall(r"(?:Table of Contents?)", corpus)
        if len(table_of_content_match) > 0:
            print('is toc present')
            # read both df replace is_include col toc-->yes
            condition_idx_ls = section_data[section_data['is_include'] == 'toc'].index.to_list(
            )
            for idx in condition_idx_ls:
                section_data.at[idx, 'is_include'] = 'Yes'

            condition_idx_ls_ = sub_section_data[sub_section_data['is_include'] == 'toc'].index.to_list(
            )
            for idx in condition_idx_ls_:
                sub_section_data.at[idx, 'is_include'] = 'Yes'

            # And remove Labs: as a section
            mask_for_complete_df = section_data['section_name'].str.contains(
                'Labs:', case=False)
            mask_for_Final_df = sub_section_data['section_name'].str.contains(
                'Labs:', case=False)

            self.section_data = section_data[~mask_for_complete_df]
            self.sub_section_data = sub_section_data[~mask_for_Final_df]

        else:
            self.sub_section_data = sub_section_data[sub_section_data['is_include'] == 'Yes']
            self.section_data = section_data[section_data['is_include'] == 'Yes']

        self.rel_sec = self.section_data[self.section_data['is_relevant']
                                         == 'Yes']['section_name'].to_list()
        self.irrel_sec = self.section_data[self.section_data['is_relevant']
                                           == 'No']['section_name'].to_list()
        self.irrel_sec_lower = list(
            set([ele.lower() for ele in self.irrel_sec]))
        self.total_sections = list(set(self.rel_sec + self.irrel_sec))
        self.sec_list = sorted(self.total_sections, key=len, reverse=True)
        self.section_subsection_intersection = self.sub_section_data[self.sub_section_data['sub_section_name'].isin(self.sec_list)]['sub_section_name'].unique()

        df_temp = sub_section_data[sub_section_data['sub_section_name'].isin(self.sec_list)].groupby('main_section')['sub_section_name'].apply(list).reset_index()
        self.sec_sub_intersec_dic = df_temp.set_index('main_section').to_dict()['sub_section_name']
        print("Section SubSection intersection dict : ", self.sec_sub_intersec_dic)

    def assign_index(df):
        """ 
        ### To check Count of word Ids == sum (count of Ids in Line)
        args:
            DataFrame: df
        return:
            if Count word Ids matched with the sum(count of Ids in Line)
                DataFrame
            else 
                None
        """
        df_word = len(df[df['BlockType'] == 'WORD'])

        df = df[df['BlockType'] == 'LINE']
        df = df.reset_index()
        df.drop(columns={'index'}, inplace=True)
        idx_len = 0
        for i, j in df.iterrows():
            for k in eval(j['Relationships']):
                idx_len += len(k['Ids'])
                df.at[i, 'word_index'] = str([idx_len-len(k['Ids']), idx_len])

        if df_word == idx_len:
            logging.info('Index count match')
            return df
        else:
            logging.error('Index count did not match')
            return None

    def text_cleanup(self, text):
        '''
            ### t1 - Remove non-alphanumeric characters (except whitespace)
            ### t2 - Convert text to lowercase, strip whitespace, and split into words
            ### t3 - Remove empty strings from the list of words
            ### stopword_count -  Count the number of stopwords in the text
            ### non_stopword_count -  Remove stopwords from the list of words
            ### non_stopword_text - Join the list of words back into a string with single spaces
            ### extra_whitespaces_removed - Replace multiple consecutive spaces with a single space
        args:
            text: string (raw text)
            stop: list of stopwords (from stop_words.csv)
        return:
            raw_str_match: string (preprocessed text)
            len_words_raw_text: int (length of the preprocessed text)
            stopword_count: int (number of stopwords in the preprocessed text)
        '''
        t1 = re.sub(r'([^A-Za-z0-9\s&]+?)', ' ', str(text))
        t1 = re.sub(r' +',' ',t1)
        t2 = t1.lower().strip().split()
        t3 = list(filter(None, t2))

        stopword_count = len([i for i in t3 if i in self.stop_words_list])
        non_stopword_count = [i for i in t3 if i not in self.stop_words_list]
        non_stopword_count_length = len(
            [i for i in t3 if i not in self.stop_words_list])
        non_stopword_text = ' '.join(non_stopword_count)
        non_extra_whitespaces_text = non_stopword_text.replace(' +', ' ').strip()

        return non_extra_whitespaces_text, non_stopword_count_length, stopword_count

    def check(self, df, i, non_extra_whitespaces_text_, stopword_count):
        non_extra_whitespaces_text = re.sub(r"\d","",non_extra_whitespaces_text_).strip()
        if len(str(non_extra_whitespaces_text)) == 0:
            pass
        non_extra_whitespaces_text = str(non_extra_whitespaces_text)
        # clean_txt_length = len(str(non_extra_whitespaces_text))
        clean_txt_length = len(str(re.sub(rf"\b({self.section_continue_terms_regex})\b","",non_extra_whitespaces_text).strip()))
        matcher_ = process.extract(re.sub(rf"\b({self.section_continue_terms_regex})\b","",non_extra_whitespaces_text).strip(), self.cleaned_sections, scorer=fuzz.token_sort_ratio)
        if matcher_:
            max_score = matcher_[0][1]
            matcher1 = [j for j in matcher_ if j[1] == max_score]
        if len(matcher1) > 0 and matcher1[0][1] >= 75:
            partial_ratio_score, matcher_idx = max((fuzz.partial_ratio(re.sub(rf"\b({self.section_continue_terms_regex})\b","",non_extra_whitespaces_text).strip(), matcher[0], processor=lambda x: x.lower().strip()), idx) for idx, matcher in enumerate(matcher1))
            average_score = (matcher1[matcher_idx][1] + partial_ratio_score)/2
            split_master_sec_by_words = matcher1[matcher_idx][0].split(' ')
            is_num = re.sub(r'([^0-9]+?)', '', str(matcher1[matcher_idx][0]))
            is_raw_text_greater = len(str(re.sub(rf"\b({self.section_continue_terms_regex})\b","",non_extra_whitespaces_text).strip()).split(' ')) >= len(
                split_master_sec_by_words)

            if len(split_master_sec_by_words) == 1 and average_score > 95 and clean_txt_length >= 3 and is_raw_text_greater and stopword_count < 3:
                self.b.append((i, non_extra_whitespaces_text, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION',
                               is_raw_text_greater, split_master_sec_by_words, non_extra_whitespaces_text.split(' ')))
                self.update_cleaned_dataframe(
                    i, non_extra_whitespaces_text, self.sec_list, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION', df)

            elif len(split_master_sec_by_words) == 1 and average_score > 87.5 and is_num == '' and clean_txt_length >= 3 and is_raw_text_greater and len(re.findall(self.section_continue_terms_regex, non_extra_whitespaces_text)) > 0 and stopword_count < 3:
                self.update_cleaned_dataframe(
                    i, non_extra_whitespaces_text, self.sec_list, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION', df)
                self.b.append((i, non_extra_whitespaces_text, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION',
                               is_raw_text_greater, split_master_sec_by_words, non_extra_whitespaces_text.split(' ')))

            elif len(split_master_sec_by_words) > 1 and average_score > 87.5 and is_num == '' and clean_txt_length >= 3 and is_raw_text_greater and stopword_count < 3:
                self.update_cleaned_dataframe(
                    i, non_extra_whitespaces_text, self.sec_list, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION', df)
                self.b.append((i, non_extra_whitespaces_text, self.sec_list[matcher1[matcher_idx][2]], average_score, 'SECTION',
                               is_raw_text_greater, split_master_sec_by_words, non_extra_whitespaces_text.split(' ')))

    def single_page_line_count_v3(self, df):
        '''
            ### Updates the line number and detects the section
        args:
            df: dataframe
        return:
            df: dataframe
        '''
        updated_df = pd.DataFrame()
        random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        for page_number in sorted(list(set(df['Page'].to_list()))):
            # print(page_number)
            count = 0
            temp = df[df['Page'] == page_number].copy()
            temp = temp.reset_index()
            th_top_ = temp['Geometry.BoundingBox.Height'].min()
            prev_line, prev_count = 0, 0
            non_extra_whitespaces_text = ""
            t_, _, stopword_count = self.text_cleanup(temp.iloc[0]['Text'])
            non_extra_whitespaces_text += t_ + " "
            temp['top_difference'] = temp['Geometry.BoundingBox.Top'].diff()
            for i in range(1, len(temp)):
                attr1 = abs(temp.iloc[i-1]['Geometry.BoundingBox.Top'] - temp.iloc[i]['Geometry.BoundingBox.Top'])
                attr2 = abs(temp.iloc[prev_line]['Geometry.BoundingBox.Top'] - temp.iloc[i]['Geometry.BoundingBox.Top'])
                t_, _ , stopword_count = self.text_cleanup(temp.iloc[i]['Text'])
                prev_count += stopword_count
                non_extra_whitespaces_text += t_ + " "
                if (i-1) >= 0 and attr1 >= th_top_ and attr2 >= th_top_ and len(t_) > 0:
                    non_extra_whitespaces_text += random_str
                    t_ += " " + random_str
                    count += 1
                    temp.loc[[k for k in range(prev_line, i)], 'updated_line_number'] = count
                    original_text = " ".join(temp.iloc[[k for k in range(prev_line, i)]]['Text'])
                    temp.loc[[k for k in range(prev_line, i)], 'stopword_count'] = prev_count - stopword_count
                    # temp.loc[[k for k in range(prev_line,i)],'non_extra_whitespaces_text'] = non_extra_whitespaces_text.replace(t_,'').strip()
                    non_extra_whitespaces_text = non_extra_whitespaces_text.replace(t_, "")
                    non_extra_whitespaces_text_without_phy = re.sub(rf"\b{self.phy_name_regex}\b", '',non_extra_whitespaces_text).strip()
                    # if not any([re.findall(rf"{key}", non_extra_whitespaces_text_without_phy, re.I) for key in section_exclude_patterns]) and not any([re.findall(rf"{key}", original_text, re.I) for key in section_exclude_patterns]):
                    if (not any([re.findall(rf"{key}", original_text, re.I) for key in self.section_exclude_patterns])) and (not original_text.islower()):
                        self.check(temp, prev_line, non_extra_whitespaces_text_without_phy, prev_count-stopword_count)
                    non_extra_whitespaces_text = t_.replace(random_str, '')
                    prev_line = i
                    prev_count = stopword_count
            count += 1
            temp.loc[[k for k in range(prev_line, len(temp))], 'updated_line_number'] = count
            temp.loc[[k for k in range(prev_line, len(temp))], 'stopword_count'] = stopword_count
            # temp.loc[[k for k in range(prev_line,len(temp))],'non_extra_whitespaces_text'] = non_extra_whitespaces_text.strip()
            temp['updated_line_number'] = temp['updated_line_number'].astype('int64')
            # print("For page ", page_number,"No of Lines : ", count)
            updated_df = pd.concat([updated_df, temp], ignore_index=True)
        updated_df.drop(columns = {'top_difference'}, inplace= True)
        return count, updated_df

    def update_cleaned_dataframe(self, i, txt, sec_subsec_list, sec_name, average_score, section_entity, df):
        '''
        ### Updates the dataframe with the section name, average score, section entity and relevant section
        
        args:
            i: int (index)
            sec_name: string (section name)
            average_score: int (average score)
            section_entity: string (section entity)
        return:
            None
        '''
        if df.iloc[i]['Text'].lower() not in self.fp_text and section_entity in ('SECTION', 'SECTION END') and df.iloc[i]['section_entity'] == None:
            section_name = sec_name
            df.loc[i, 'entity'] = section_name
            df.loc[i, 'section'] = section_name
            df.loc[i, 'score'] = average_score
            relevancy = 'No' if sec_name.lower() in self.irrel_sec_lower else 'Yes'
            df.loc[i, 'section_entity'] = section_entity
            df.loc[i, 'is_relevant'] = relevancy

        elif section_entity =='SUB SECTION':
            section_name = sec_name
            df.loc[i, 'sub_section_entity'] = section_name
            df.loc[i, 'sub_section_entity_score'] = average_score
            relevancy = 'No' if sec_name.lower() in self.irrel_sub_sec else 'Yes'
            df.loc[i, 'section_entity'] = section_entity
            df.loc[i, 'is_sub_section_relevant'] = relevancy

    def get_lookup_table_df(self, df):
        """
        ### The function takes a DataFrame as an input and adds two new columns: Start and End. 
        ### These columns represent the starting and ending positions of the text in each row (Cumulatively).
        ### The function returns the modified DataFrame as an output. 
        args:
            df: DataFrame
        return:
            df: DataFrame
        """
        df['text_len'] = df['Text'].str.len()
        df['Start'] = df['text_len'].shift(
            fill_value=0).cumsum() + df.apply(lambda row: row.name, axis=1)
        df['End'] = df['Start']+df['text_len']
        df.drop('text_len',  axis=1, inplace=True)
        return df

    def get_corpus(self, d1):
        """
        ### Get the corpus of the entire DataFrame
        args:
            df: DataFrame
        return:
            corpus: Text
        """
        corpus = ' '.join(d1['Text'].apply(lambda x: str(x)))
        return corpus

    def supress_date(self, date_tags_list, suppress_date_tag):
        """
        ### To Suppress the Date Tags
        args:
            date_tags_list: List of tuples. Tuple contains the date, time and the Date tag.
            suppress_date_tag: List of Date tags we're not interested.
        return:
            date_tags_true: List of Tuples. Dates we're interested
        """
        date_tags_true = []
        suppress_date_ls = list(map(lambda x: x.lower(), suppress_date_tag))
        for i in date_tags_list:
            if str(i[2]).lower() not in suppress_date_ls:
                date_tags_true.append(i)
        return date_tags_true

    def sub_sec_detection(self, i, non_extra_whitespaces_text, all_sub_sections_list_sorted, cleaned_subsec, irrel_sub_sec, non_stopword_count_length, df):
        """
        ### Check whether the text is Sub Section
        args:
            i: index
            non_extra_whitespaces_text: text after clean up
            all_sub_sections_list_sorted: List of Sub Sections.
            cleaned_subsec: List of Sub Sections after clean up
            irrel_sub_sec: List of irrelavant Sub Sections.
            non_stopword_count_length: length of non_stopword_count
            df: DataFrame
        return:
            None
        """
        matcher = process.extractOne(
            non_extra_whitespaces_text, cleaned_subsec, scorer=fuzz.token_sort_ratio)

        if matcher[1] >= 75:
            ratio_partial = round(fuzz.partial_ratio(
                str(non_extra_whitespaces_text), matcher[0]), 2)
            average_score = round((matcher[1] + ratio_partial)/2)
            original_sub_sec = all_sub_sections_list_sorted[matcher[2]]
            if len(original_sub_sec.split()) == 1 and average_score > 95 and (non_stopword_count_length >= len(original_sub_sec.split()) or average_score >= 95):
                self.update_cleaned_dataframe(
                    i, non_extra_whitespaces_text, self.sec_list, cleaned_subsec[matcher[2]], average_score, 'SUB SECTION', df)

            elif len(original_sub_sec.split()) > 1 and average_score > 87.5 and (non_stopword_count_length >= len(original_sub_sec.split()) or average_score >= 87.5):
                self.update_cleaned_dataframe(
                    i, non_extra_whitespaces_text, self.sec_list, cleaned_subsec[matcher[2]], average_score, 'SUB SECTION', df)

    def get_result(self, df, index, range1, range2, suppress_date_tags):
        """
        ### Get the date in the given range of corpus
        args:
            df: DataFrame
            index: Index
            range1: Start Index
            range2: End Index
            suppress_date_tags: List of Date Tags to Suppress.
        return:
            section_dates: returns the Dates
        """
        restrict_dict = {'day': (1, 31), 'month': (
            1, 12), 'year': (2000, 2030), 'year_': (1, 99)}


        corpus = self.get_corpus(df[index+range1:index+range2])
        # corpus = corpus.lower().replace(df.iloc[index]['Physician Name'].lower(), '') if str(df.iloc[index]['Physician Name']) != 'nan' else corpus
        corpus = corpus.lower().replace(df.iloc[index]['Physician Name'].lower(), '') if str(df.iloc[index]['Physician Name']).lower() not in ['nan', 'none'] else corpus
        section_dates = self.supress_date(DatetimeExtractor.get_date_time_from_corpus_v2(
            corpus.lower(), self.date_tags, restrict_dict=restrict_dict), suppress_date_tags)
        return section_dates

    def update_cleaned_dataframe_v2(self, i, txt, sec_subsec_list, sec_name, average_score, section_entity, irr_i, df):
        '''
        ### If a particular text is Sub Section but we detect it as a Section, we update it as Sub Section here.
        args:
            i: int (index)
            sec_name: string (section name)
            average_score: int (average score)
            section_entity: string (section entity)
        return:
            None
        '''
        if section_entity in ('SUB SECTION'):
            df.loc[irr_i, 'entity'] = ''
            df.loc[irr_i, 'section'] = ''
            df.loc[i, 'is_relevant'] = str('nan')
            df.loc[i, 'score'] = np.NaN

            section_name = sec_name
            df.loc[i, 'sub_section_entity'] = section_name
            df.loc[i, 'sub_section_entity_score'] = average_score
            relevancy = 'No' if sec_name.lower() in self.irrel_sub_sec else 'Yes'
            df.loc[i, 'section_entity'] = section_entity
            df.loc[i, 'is_sub_section_relevant'] = relevancy

    def sub_sec_detection_v2(self, i, non_extra_whitespaces_text, all_sub_sections_list_sorted, cleaned_subsec, irrel_sub_sec, non_stopword_count_length, irr_indexes, df):
        '''
        ### If a particular text is detected as Section but within main section, if it is not a section we update it.
        args: 
            i: int (index)
            non_extra_whitespaces_text: text after clean up
            all_sub_sections_list_sorted: List of Sub Sections.
            cleaned_subsec: List of Sub Sections after clean up
            irrel_sub_sec: List of irrelavant Sub Sections.
            non_stopword_count_length: length of non_stopword_count
            irr_indexes: irrelavant indexes.
            df: DataFrame
        return:
            None
        '''
        matcher = process.extractOne(
            non_extra_whitespaces_text, cleaned_subsec, scorer=fuzz.token_sort_ratio)

        if matcher[1] >= 75:
            ratio_partial = round(fuzz.partial_ratio(
                str(non_extra_whitespaces_text), matcher[0]), 2)
            average_score = round((matcher[1] + ratio_partial)/2)
            original_sub_sec = all_sub_sections_list_sorted[matcher[2]]
            if len(original_sub_sec.split()) == 1 and average_score > 95 and (non_stopword_count_length >= len(original_sub_sec.split()) or average_score >= 95):
                self.update_cleaned_dataframe_v2(i, non_extra_whitespaces_text, self.sec_list,
                                                 cleaned_subsec[matcher[2]], average_score, 'SUB SECTION', irr_indexes, df)

            elif len(original_sub_sec.split()) > 1 and average_score > 87.5 and (non_stopword_count_length >= len(original_sub_sec.split()) or average_score >= 87.5):
                self.update_cleaned_dataframe_v2(i, non_extra_whitespaces_text, self.sec_list,
                                                 cleaned_subsec[matcher[2]], average_score, 'SUB SECTION', irr_indexes, df)

            else:
                df.loc[irr_indexes, 'entity'] = ''
                df.loc[irr_indexes, 'section'] = ''
                df.loc[i, 'is_relevant'] = str('nan')
                df.loc[i, 'score'] = np.NaN
                df.loc[i, 'section_entity'] = str('nan')
        else:
            df.loc[irr_indexes, 'entity'] = ''
            df.loc[irr_indexes, 'section'] = ''
            df.loc[i, 'is_relevant'] = str('nan')
            df.loc[i, 'score'] = np.NaN
            df.loc[i, 'section_entity'] = str('nan')

    def detect_section_end(self, df):

        self.section_end_regex = "|".join(self.section_end_patterns)
        df['flag'] = df['Text'].str.lower().str.contains(rf'{self.section_end_regex}', regex=True).astype(int)
        df['section_end_pattern'] = df['Text'].str.extract(f'(\b{self.section_end_regex}\b)', flags=re.IGNORECASE)
        df['section_end_pattern'] = df['section_end_pattern'].apply(lambda x : str(x).lower())
        df['section_entity'] = df.apply(lambda row: 'SECTION END' if row['flag'] == 1 else None, axis=1)
        df['is_relevant'] = df.apply(lambda row: 'Yes' if row['flag'] == 1 else None, axis=1)
        df['score'] = df.apply(lambda row: 100 if row['flag'] == 1 else None, axis=1)
        df.drop(columns = {'flag'}, inplace = True)

    def section_continue_terms(self, df):
        r1 = r"(\(?\s?\bcontinued\b\s?\)?)"
        r2 = r"(\(?\s?group\s?\d\s?of\s?\d\)|\(?\s?version\s?\d\s?of\s?\d\))"
        df['flag1'] = df['Text'].str.lower().str.contains(rf'{r1}', regex=True).fillna(0).astype(int)
        df['flag2'] = df['Text'].str.lower().str.contains(rf'{r2}', regex=True).fillna(0).astype(int)
        df['section_continue_terms'] = df['Text'].str.extract(f'{r1}', flags=re.IGNORECASE)
        df['section_continue_terms_exclude'] = df['Text'].str.extract(f'{r2}', flags=re.IGNORECASE)
        # idx = df[(df['flag1'] == 1) & (df['flag2'] == 1) & (df['section_entity'] == 'SECTION')].index
        idx = df[(df['flag1'] == 1) & (df['section_entity'] == 'SECTION')].index
        df.loc[idx, 'section_entity'] = None
        df.loc[idx, 'entity'] = None
        df.loc[idx, 'section'] = None

    def section_logic(self, df):
        """
        ### Detects the text as Section and Updates it.
        args:
            df: DataFrame
        return:
            None
        """
        self.section_start_time = time.time()
        logging.info('Detecting SECTIONs and SECTION END')
        self.section_end_regex = "|".join(self.section_end_patterns)
        self.phy_name_ls = [i.lower() for i in self.df_end[self.df_end['physician_credential'].isin([i for i in self.df_end['physician_credential'].unique() if str(i).lower() not in ['', 'nan', 'none']])]['Physician Name'].unique()]
        self.phy_name_ls_proper = [re.sub(r'([^A-Za-z0-9\s]+?)', ' ', str(i)).strip() for i in self.phy_name_ls]
        self.phy_name_ls_proper = [re.sub(r' +', ' ',i) for i in self.phy_name_ls_proper]
        self.phy_name_ls_proper = list(set(self.phy_name_ls_proper))
        self.phy_name_ls_proper = sorted(list(set(self.phy_name_ls_proper)), key=len, reverse=True)
        self.phy_name_ls_proper = [i for i in self.phy_name_ls_proper if len(i) >= 3]
        self.phy_name_regex = "|".join(self.phy_name_ls_proper)
        self.b = []
        logging.info('Adding Line numbers')
        _, df = self.single_page_line_count_v3(df)
        logging.info('Added Line numbers')

        print("The length of b is : ", len(self.b))
        print("Total number of Sections detected : ",df[df['section_entity'] == 'SECTION'].shape[0])
        print("Total number of Section ends detected : ",df[df['section_entity'] == 'SECTION END'].shape[0])
        # print(df[df['section_entity'] == 'SECTION']['entity'].value_counts())
        print(dict(zip(df[df['section_entity'] == 'SECTION']['entity'].value_counts().index,df[df['section_entity'] == 'SECTION']['entity'].value_counts().values)))
        self.section_end_time = time.time()
        logging.info('Detected SECTIONs and SECTION END')
        return df

    def suppress_section(self, df):
        """
        ### Suppress the false positives in the section.
        args:
            df: DataFrame
        return:
            None
        """
        def print_suppress_ls(idx_ls, str_constant):
            k = df.iloc[idx_ls][['Page','updated_line_number']].copy()
            keys = [f"{row['Page']}_{row['updated_line_number']}" for index, row in k.iterrows()]
            values = [" ".join(df[(df['updated_line_number'] == df.iloc[i]['updated_line_number']) & (df['Page'] == df.iloc[i]['Page'])]['Text']) for i in idx_ls]
            print(f"{str_constant}suppress sections : {dict(zip(keys, values))}")

        page_ls = set(df[df['section_entity'] == "SECTION"]['Page'].values)

        suppress_section_idx = []
        for page in page_ls:
            idx_ls = df[(df['Page'] == page) & (
                df['section_entity'] == "SECTION")].index
            th_top = np.average(df[df['Page'] == page]
                                ['Geometry.BoundingBox.Height'])
            if len(idx_ls) > 0:
                for idx in idx_ls:
                    # Check for the height of the current txt, w.r.t previous and next text, if the height is less than average consider it as in one line
                    if (abs(df.iloc[idx]['Geometry.BoundingBox.Top'] - df.iloc[idx-1]['Geometry.BoundingBox.Top']) < th_top or
                            abs(df.iloc[idx]['Geometry.BoundingBox.Top'] - df.iloc[idx+1]['Geometry.BoundingBox.Top']) < th_top) and \
                            not (df.iloc[idx]['Text'].isupper()):
                        suppress_section_idx.append(idx)

        updated_ss_idx = []
        for i in suppress_section_idx:
            temp = df[df['Page'] == df.iloc[i]['Page']].copy()
            if df.iloc[i]['Geometry.BoundingBox.Left'] >= 0.48:
                updated_ss_idx.append(i)
        print_suppress_ls(suppress_section_idx, "")
        print_suppress_ls(updated_ss_idx, "updated ")
        df.loc[updated_ss_idx, 'section_entity'] = None
        df.loc[updated_ss_idx, 'entity'] = None
        df.loc[updated_ss_idx, 'section'] = None
        df.loc[updated_ss_idx, 'score'] = np.nan

        self.section_continue_terms(df)

        df['entity'] = df['entity'].mask(df['entity'] == 'nan', None).ffill()
        df['entity'] = df['entity'].mask(df['entity'] == 'n', None).ffill()
        df['entity'] = df['entity'].mask(df['entity'] == '', None).ffill()

        df['section'] = df['section'].mask(
            df['section'] == 'nan', None).ffill()
        df['section'] = df['section'].mask(df['section'] == 'n', None).ffill()
        df['section'] = df['section'].mask(df['section'] == '', None).ffill()

    def get_main_section_dict(self, df):
        """
        ### Map each section to Main Section.
        args:
            df: DataFrame
        return:
            None
        """
        df1 = df[['Page', 'Text', 'section_entity', 'word_index',
                  'entity', 'is_relevant']].drop_duplicates()
        df1 = df1[df1['is_relevant'] == 'Yes']
        detected_sections = list(set(df1['entity'].dropna()))

        df['main_section'] = None
        self.main_sec_dict = {}
        for i, j in self.sub_section_data.iterrows():
            for k in detected_sections:
                if j['section_name'] == k:
                    self.main_sec_dict[j['section_name']] = j['main_section']

        df['main_section'] = df['entity'].replace(self.main_sec_dict)

    def sub_section_logic(self, df):
        """
        ### Detects the text as Sub Section and Updates it.
        args:
            df: DataFrame
        return:
            None
        """
        self.sub_section_start_time = time.time()
        logging.info('Detecting SUB SECTIONs')
        for k, v in list(set(self.main_sec_dict.items())):
            i = self.main_sec_dict.get(k)
            # Step 1: Find the index of the mapped main section
            # Step 2: Get subsections related to the mapped main section
            # Step 3: Get indexes of rows in relevant_sections_df where 'section' matches 'i'
            # Step 4: Extract irrelevant subsections
            # Step 5: Extract all subsections and sort them by length in reverse order
            # Step 6: Return the extracted information
            mapped_main_section = list(set(
                self.sub_section_data[self.sub_section_data['section_name'] == k]['main_section']))
            logging.info(
                f'Detecting SUB SECTIONs for {k} --> {mapped_main_section}')
            if len(mapped_main_section) > 0:
                mapped_main_section = mapped_main_section[0]

                sec_idxs = df[df['entity'] == k].index.tolist()

                all_subsec_df = self.sub_section_data[self.sub_section_data['main_section']
                                                      == mapped_main_section]

                rel_sub_sec_list = all_subsec_df[all_subsec_df['is_relevant']
                                                 == 'Yes']['sub_section_name'].to_list()
                rel_sub_sec_list = [ele.lower().strip()
                                    for ele in rel_sub_sec_list if str(ele).strip() != '' and str(ele).lower() != 'nan']

                self.irrel_sub_sec = all_subsec_df[all_subsec_df['is_relevant']
                                                   == 'No']['sub_section_name'].to_list()
                self.irrel_sub_sec = [ele.lower()
                                      for ele in self.irrel_sub_sec]

                all_sub_sections = all_subsec_df['sub_section_name'].to_list()
                all_sub_sections = [i for i in all_sub_sections if str(i).strip() != '' and str(i).lower() != 'nan']
                all_sub_sections_list_sorted = sorted(
                    all_sub_sections, key=len, reverse=True)

                if len(mapped_main_section) == 0:
                    logging.error(
                        'No mapped main section found for the section {}'.format(i))

                cleaned_subsec = [self.text_cleanup(
                    ele)[0] for ele in all_sub_sections_list_sorted]
                cleaned_subsec = list(set(cleaned_subsec))
                if len(cleaned_subsec) == 0:
                    continue
                for idx in sec_idxs:
                    raw_cleaned_txt = str(df['Text'][idx])

                    if ':' in raw_cleaned_txt and str(df['section_entity'][idx]) == 'None':
                        for txt_split in raw_cleaned_txt.split(":"):
                            # txt_split = raw_cleaned_txt.split(":")[0]
                            txt_colon_split = str(txt_split).strip()
                            non_extra_whitespaces_text, non_stopword_count_length, stopword_count = self.text_cleanup(
                                txt_colon_split)
                            self.sub_sec_detection(idx, non_extra_whitespaces_text, all_sub_sections_list_sorted,
                                                   cleaned_subsec, self.irrel_sub_sec, non_stopword_count_length, df)

                    elif str(df['section_entity'][idx]) == 'None':
                        non_extra_whitespaces_text, non_stopword_count_length, stopword_count = self.text_cleanup(
                            raw_cleaned_txt)
                        matcher = process.extractOne(
                            non_extra_whitespaces_text, cleaned_subsec, scorer=fuzz.token_sort_ratio)
                        ratio_partial = round(fuzz.partial_ratio(
                            str(non_extra_whitespaces_text), matcher[0]), 2)
                        average_score = round((matcher[1] + ratio_partial)/2)

                        if stopword_count <= 2 or average_score > 98:
                            self.sub_sec_detection(idx, non_extra_whitespaces_text, all_sub_sections_list_sorted,
                                                   cleaned_subsec, self.irrel_sub_sec, non_stopword_count_length, df)
                # time.sleep(5)

        logging.info('Detected SUB SECTIONs')
        self.sub_section_end_time = time.time()

    def is_section_end_present(self, df):
        # idx = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (df['section_end_pattern'] != 'performed by')].index.to_list()
        idx = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].index.to_list()
        idx = idx[::-1]
        prev_idx = idx[0]
        prev_entity = None
        for idx in idx[1:]:
            if df.iloc[prev_idx]['section_entity'] == 'SECTION END':
                if df.iloc[prev_idx]["entity"].lower() == 'nan':
                    df.at[prev_idx,'is_section_end_present'] = 0
                    continue
                else:
                    prev_entity =  df.iloc[prev_idx]['entity']

            if df.iloc[prev_idx]['entity'] == df.iloc[idx]['entity'] and prev_entity and df.iloc[prev_idx]['entity'] == prev_entity:
                df.at[prev_idx,'is_section_end_present'] = 1
                prev_idx = idx
                continue
            elif df.iloc[prev_idx]['entity'] != df.iloc[idx]['entity'] and prev_entity and df.iloc[prev_idx]['entity'] == prev_entity:
                df.at[prev_idx,'is_section_end_present'] = 1
                prev_idx = idx
                continue
            else:
                if df.iloc[prev_idx]['entity'] in self.section_subsection_intersection:
                    df.at[prev_idx,'is_section_end_present'] = 2
                else:
                    df.at[prev_idx,'is_section_end_present'] = 0
                    prev_entity = ''
                prev_idx = idx
        df['is_section_end_present'] = df['is_section_end_present'].bfill()

    def is_section_end_presentv1(self, df, sec_sub_intersec_dic, col):
        df['is_section_end_present'] = None
        if df[df['section_entity'] == 'SECTION'].shape[0] > 0:
            df_temp = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END")]
            # idx_ = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (df['section_end_pattern'] != 'performed by')].index.to_list()
            idx_ = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].index.to_list()
            ls_check = [value for values in sec_sub_intersec_dic.values() for value in values]
            idx_ls = idx_[::-1]
            prev_idx = idx_ls[0]
            entity_cache = False
            en_chache = False
            for idx in idx_ls[1:]:
                if df.iloc[prev_idx]['section_entity'] == 'SECTION END':
                    if df.iloc[prev_idx][col] == 'NaN':
                        df.at[prev_idx,'is_section_end_present'] = 0
                        continue
                    else:
                        entity_cache =  df.iloc[prev_idx][col]
                        en_chache = df.iloc[prev_idx]['entity']

                if df.iloc[prev_idx][col] == df.iloc[idx][col] and entity_cache and df.iloc[prev_idx][col] == entity_cache:
                    df.at[prev_idx,'is_section_end_present'] = 1
                    prev_idx = idx
                    continue
                elif df.iloc[prev_idx][col] != df.iloc[idx][col] and entity_cache and df.iloc[prev_idx][col] == entity_cache:
                    df.at[prev_idx,'is_section_end_present'] = 1
                    prev_idx = idx
                    continue
                elif entity_cache and en_chache and en_chache in [sec_sub_intersec_dic.get(df.iloc[prev_idx]['main_section']) if sec_sub_intersec_dic.get(df.iloc[prev_idx]['main_section']) else [False]][0]:
                    df.at[prev_idx,'is_section_end_present'] = 1
                    entity_cache =  df.iloc[prev_idx][col]
                    en_chache = ''
                    prev_idx = idx
                    continue
                else:
                    if df.iloc[prev_idx]['entity'] in ls_check:
                        df.at[prev_idx,'is_section_end_present'] = 2
                    else:
                        df.at[prev_idx,'is_section_end_present'] = 0
                        entity_cache = False
                        en_chache = False
                    prev_idx = idx
            idx2, idx1 = int(idx_ls[-2]) if idx_ls and len(idx_ls)>=2 else None, int(idx_ls[-1]) if idx_ls and len(idx_ls)>=1 else None 
            if idx2 and (df.iloc[idx2]['section_entity'] == 'SECTION') and (df.iloc[idx2]['entity'] == df.iloc[idx1]['entity']):
                df.at[idx1, 'is_section_end_present'] = df.iloc[idx2]['is_section_end_present'] 
            if idx2 and idx1 and (df.iloc[idx2]['section_entity'] == 'SECTION') and (df.iloc[idx2]['entity'] in [sec_sub_intersec_dic.get(df.iloc[idx1]['main_section']) if sec_sub_intersec_dic.get(df.iloc[idx1]['main_section']) else [False]][0]):
                idx_temp = None
                idx__ = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].index
                for i in idx__[idx__ > idx1]:
                    if df.iloc[i]['is_section_end_present'] in [0, 1]:
                        # print(i)
                        idx_temp = i
                        break
                df.at[idx1, 'is_section_end_present'] = 1 if idx_temp and df.iloc[idx_temp]['main_section'] == df.iloc[idx1]['main_section'] else 0
            # idx_ = df[(df['section_entity'].isin(["SECTION", "SECTION END"])) & (df['is_section_end_present'].isna())].index
            # idx_ = df[(df['section_entity'].isin(["SECTION", "SECTION END"])) & (df['section_end_pattern'] != 'performed by')& (df['is_section_end_present'].isna())].index
            idx_ = df[(df['section_entity'].isin(["SECTION", "SECTION END"])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))& (df['is_section_end_present'].isna())].index
            df.loc[idx_, 'is_section_end_present'] = 0
        else:
            pass

    def get_list(self, main_section):
        if main_section in self.sec_sub_intersec_dic.keys():
            return self.sec_sub_intersec_dic.get(main_section)
        else:
            return []
 
    def update_main_section(self, df):
        """
        ### Update the Main Section of Each Section.
        ### Update the Sections to Sub Sections under each main section
        args:
            df: DataFrame
        return:
            None
        """
        # ------------------------------- Section -> Main Section Update-------------------------------
        logging.info('Updating the Main Section')
        df_updated = df[(df['section_entity'] == "SECTION") | (
            df['section_entity'] == "SECTION END") | (df['section_entity'] == "SUB SECTION")].copy()
        df_updated_idxs = df_updated.index.to_list()

        prev_main_section = None
        need_to_change_df = []
        for idx in range(len(df_updated)):
            if df_updated.iloc[idx]['main_section'] != prev_main_section and prev_main_section is not None:
                if df_updated.iloc[idx-1]['section_entity'] != "SECTION END":
                    need_to_change_df.append(df_updated.iloc[idx])
                    continue
                if df_updated.iloc[idx]['section_entity'] == "SECTION END":
                    need_to_change_df.append(df_updated.iloc[idx])
                    continue
                prev_main_section = df_updated.iloc[idx]['main_section']
            else:
                prev_main_section = df_updated.iloc[idx]['main_section']

        need_to_change_df_idx = pd.DataFrame(need_to_change_df).index.to_list()

        idx_ss_ls = list(set(need_to_change_df_idx).symmetric_difference(
            set(df_updated.index.to_list())))
        for idx in idx_ss_ls:
            df.at[idx, 'updated_main_section'] = df.iloc[idx]['main_section']
        df['updated_main_section'] = df['updated_main_section'].replace(
            np.NaN, 'nan')
        df['updated_main_section'] = df['updated_main_section'].mask(
            df['updated_main_section'] == 'nan', None).ffill()
        # --------------------------------------------------------------------------------------------

    def update_main_section_v1(self, df):
        # df_updated = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (df['section_end_pattern'] != 'performed by')].copy()
        df_updated = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].copy()
        idx_all = df_updated.index
        prev_idx = None
        for i in idx_all:
            if (prev_idx == None) & (df.iloc[i]['is_section_end_present'] == 2):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif ((df.iloc[i]['is_section_end_present'] == 0) | (df.iloc[i]['is_section_end_present'] == 1)):
                df.at[i, 'updated_is_section_end_present'] = df.iloc[i]['is_section_end_present']
            elif (df.iloc[i]['is_section_end_present'] == 2) & (df.iloc[prev_idx]['updated_is_section_end_present'] == 0):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif (df.iloc[i]['is_section_end_present'] == 2) & (df.iloc[prev_idx]['section_entity'] == 'SECTION END'):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif (df.iloc[i]['is_section_end_present'] == 2) & (df.iloc[prev_idx]['section_entity'] == 'SECTION'):
                df.at[i, 'updated_is_section_end_present'] = 1
            prev_idx = i
        ignore_idx = df[(df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'] == 1)].index
        idx_ss_ls = set(idx_all).difference(set(ignore_idx))
        df['updated_is_section_end_present'] = df['updated_is_section_end_present'].ffill()
        
        for idx in idx_ss_ls:
            df.at[idx, 'updated_main_section'] = df.iloc[idx]['main_section']

        df['updated_main_section'] = df['updated_main_section'].replace(np.NaN, 'nan')
        df['updated_main_section'] = df['updated_main_section'].mask(df['updated_main_section'] == 'nan', None).ffill()

    def update_main_section_v2(self, df):
        # df_updated = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (df['section_end_pattern'] != 'performed by')].copy()
        df_updated = df[(df['section_entity'] == "SECTION") | (df['section_entity'] == "SECTION END") & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].copy()
        idx_all = df_updated.index
        prev_idx = None
        if "updated_is_section_end_present" not in df.columns:
            df['updated_is_section_end_present'] = None
        for i in idx_all:
            if (prev_idx == None) & (df.iloc[i]['is_section_end_present'] == 2):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif ((df.iloc[i]['is_section_end_present'] == 0) | (df.iloc[i]['is_section_end_present'] == 1)):
                df.at[i, 'updated_is_section_end_present'] = df.iloc[i]['is_section_end_present']
            elif (df.iloc[i]['is_section_end_present'] == 2) and (df.iloc[prev_idx]['updated_is_section_end_present'] == 0) and (df.iloc[i]['entity'] in self.get_list(df.iloc[prev_idx]['main_section'])):
                df.at[i, 'updated_is_section_end_present'] = 3
                continue
            elif (df.iloc[i]['is_section_end_present'] == 2) and (df.iloc[prev_idx]['updated_is_section_end_present'] == 0):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif (df.iloc[i]['is_section_end_present'] == 2) and (df.iloc[prev_idx]['section_entity'] == 'SECTION END'):
                df.at[i, 'updated_is_section_end_present'] = 0
            elif (df.iloc[i]['is_section_end_present'] == 2) and (df.iloc[prev_idx]['section_entity'] == 'SECTION'):
                df.at[i, 'updated_is_section_end_present'] = 1
            prev_idx = i
        if not idx_all.empty:
            ignore_idx = df[((df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'] == 1))|((df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'] == 3))].index
            idx_ss_ls = set(idx_all).difference(set(ignore_idx))
            df['updated_is_section_end_present'] = df['updated_is_section_end_present'].ffill()
            
            for idx in idx_ss_ls:
                df.at[idx, 'updated_main_section'] = df.iloc[idx]['main_section']

            df['updated_main_section'] = df['updated_main_section'].replace(np.NaN, 'nan')
            df['updated_main_section'] = df['updated_main_section'].mask(df['updated_main_section'] == 'nan', None).ffill()
 
    def update_main_section_v3(self, df):
        if 'updated_is_section_end_present' in df.columns and "updated_main_section" in df.columns:
            ls_is_sec_end_present = [i for i in df['updated_is_section_end_present'].unique().tolist() if str(i).lower().strip() not in ['nan', 'none']]
            flag1, flag2 = 0, 0
            if 1 in ls_is_sec_end_present:
                flag1 = 1
                res_ = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern)) & (df['updated_is_section_end_present'] == 1)].copy()
            if (0 in ls_is_sec_end_present) or (3 in ls_is_sec_end_present):  
                flag2 = 1  
                idx_ = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern)) & (df['updated_is_section_end_present'] != 1)].index
                val_ = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern)) & (df['updated_is_section_end_present'] != 1)]['entity'].values
                upd_main_sec_val_ = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern)) & (df['updated_is_section_end_present'] != 1)]['updated_main_section'].values
            if flag1 == 1:
                res_.loc[:, 'updated_section_entity'] = res_['section_entity'].shift(-1, axis = 0)
                res_.loc[:, 'flag'] = res_['updated_section_entity'] == res_['section_entity']
                idx = res_[(res_['section_entity'] == 'SECTION END') & (res_['flag'] == False)].index
                r = res_[(res_['section_entity'] == 'SECTION END') & (res_['flag'] == False)].shape[0]
                res_.loc[idx, 'rank'] = list(range(1, r+1))
                res_['rank'].fillna(method = 'bfill', inplace = True)
                res_.rename(columns={'updated_main_section':'prev_main_section'}, inplace=True)
                res_.rename(columns={'entity':'prev_entity'}, inplace=True)
                res_['entity'] = res_.groupby('rank')['prev_entity'].transform('first')
                res_['updated_main_section'] = res_.groupby('rank')['prev_main_section'].transform('first')
            
            if flag1 == 1 and flag2 == 1:
                df.rename(columns={'entity':'prev_entity'}, inplace=True)
                df.rename(columns={'updated_main_section':'prev_main_section'}, inplace=True)
                df.loc[res_.index, 'entity'] = res_['entity'].values
                df.loc[res_.index, 'updated_main_section'] = res_['updated_main_section'].values
                df.loc[idx_, 'entity'] = val_
                df.loc[idx_, 'updated_main_section'] = upd_main_sec_val_
                sec_idx = df[(df['entity'] != df['prev_entity']) & (df['section_entity'] == 'SECTION')].index
                df.loc[sec_idx, 'section_entity'] = 'SUB SECTION'
                df.loc[sec_idx, 'sub_section_entity'] = df.iloc[sec_idx]['prev_entity'].values
            if flag1 == 0 and flag2 == 1:
                df.rename(columns={'entity':'prev_entity'}, inplace=True)
                df.rename(columns={'updated_main_section':'prev_main_section'}, inplace=True)
                df.loc[idx_, 'entity'] = val_
                df.loc[idx_, 'updated_main_section'] = upd_main_sec_val_

            df['entity'].fillna(method = 'ffill', inplace = True)
            df['updated_main_section'].fillna(method = 'ffill', inplace = True)
            df['section'] = df['entity']

    def update_section_to_sub_section(self, df):
        # ---------------Update the Sections to Sub Sections under each main section.-----------------
        try:
            check_column = df.iloc[0]['updated_main_section']
        except:
            df['updated_main_section'] = df['main_section']
        if not df[df['section_entity'] == 'SECTION'].index.empty:
            main_sec_ls = df[(df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'].isin([1, 3])) & (df['section_entity'] == 'SECTION')]['updated_main_section'].unique()
            for i in main_sec_ls:
                sec_ls = self.sub_section_data[self.sub_section_data['main_section'] == i]['section_name'].unique()
                ls_ = df[(df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'].isin([1, 3])) & (df['section_entity'] == 'SECTION') & (df['updated_main_section'] == i)]['entity'].unique()
                for j in ls_:
                    indexes = df[(df['is_section_end_present'] == 2) & (df['updated_is_section_end_present'].isin([1,3])) & (df['section_entity'] == 'SECTION') & (df['updated_main_section'] == i) & (df['entity'] == j)].index
                    irr_indexes = df[(df['updated_main_section'] == i) & (df['entity'] == j)].index
                    if j not in sec_ls and len(sec_ls) > 0:
                        raw_cleaned_txt = str(j)

                        all_subsec_df = self.sub_section_data[self.sub_section_data['main_section'] == i]

                        rel_sub_sec_list = all_subsec_df[all_subsec_df['is_relevant']== 'Yes']['sub_section_name'].to_list()
                        rel_sub_sec_list = [ele.lower().strip()for ele in rel_sub_sec_list if str(ele).strip() != '' and str(ele).lower() != 'nan']
                        self.irrel_sub_sec = all_subsec_df[all_subsec_df['is_relevant'] == 'No']['sub_section_name'].to_list()
                        self.irrel_sub_sec = [ele.lower()for ele in self.irrel_sub_sec]

                        all_sub_sections = all_subsec_df['sub_section_name'].to_list()
                        all_sub_sections = [i for i in all_sub_sections if str(i) != '' and str(i).lower() != 'nan']
                        all_sub_sections_list_sorted = sorted(all_sub_sections, key=len, reverse=True)

                        cleaned_subsec = [self.text_cleanup(ele)[0] for ele in all_sub_sections_list_sorted]
                        if ':' in raw_cleaned_txt:
                            txt_colon_split = str(raw_cleaned_txt).strip()
                            non_extra_whitespaces_text, non_stopword_count_length, stopword_count = self.text_cleanup(
                                txt_colon_split)
                            self.sub_sec_detection_v2(indexes, non_extra_whitespaces_text, all_sub_sections_list_sorted,
                                                    cleaned_subsec, self.irrel_sub_sec, non_stopword_count_length, irr_indexes, df)

                        else:
                            non_extra_whitespaces_text, non_stopword_count_length, stopword_count = self.text_cleanup(raw_cleaned_txt)
                            non_extra_whitespaces_text = str(non_extra_whitespaces_text)
                            matcher = process.extractOne(non_extra_whitespaces_text, cleaned_subsec, scorer=fuzz.token_sort_ratio)
                            ratio_partial = round(fuzz.partial_ratio(str(non_extra_whitespaces_text), matcher[0]), 2)
                            average_score = round((matcher[1] + ratio_partial)/2)

                            if stopword_count <= 2 or average_score > 98:
                                self.sub_sec_detection_v2(indexes, non_extra_whitespaces_text, all_sub_sections_list_sorted,
                                                        cleaned_subsec, self.irrel_sub_sec, non_stopword_count_length, irr_indexes, df)

        df['entity'] = df['entity'].mask(df['entity'] == '', None).ffill()
        df['section'] = df['section'].mask(df['section'] == '', None).ffill()
        # --------------------------------------------------------------------------------------------

    def relevant_date_tags(self, ls, required_date_tags ):
        final_date_tags = []
        for i in ls:
            if i[2] in required_date_tags:
                final_date_tags.append(i)
        return final_date_tags

    def suppress_date(self, dates):
        final_dates = []
        for date in dates:
            if len(re.sub(r'\d','', date[0])) > 0:
                final_dates.append(date)
        return final_dates

    def section_dates(self, df):
        """
        ### Logic for dates at Section
        args:
            df: DataFrame
        return:
            None
        """
        lookup_section_start = df[df['section_entity'] == "SECTION"].index
        lookup_section_end = df[(df['section_entity'] == 'SECTION END') & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern))].index

        crp = self.get_corpus(df)
        p_obj = Postprocess(self.adm_dis_tag)
        self.yr_ = p_obj.get_adm_discharge_date(crp)

        section_dates_ls = []
        result = []
        page_list = sorted(df['Page'].unique().tolist())
        for idx in lookup_section_start:
            try:
                range2_ = min(lookup_section_end[lookup_section_end>idx][0], lookup_section_start[lookup_section_start>idx][0]) - idx
            except:
                range2_ = 10
            range2 = min(10, range2_)
            section_date__ = self.get_result(
                df, idx, 0, range2, self.suppress_date_tag_section)
            section_date_ = self.relevant_date_tags(section_date__,self.section_date_tags)
            if len(section_date_) > 0:
                if eval(str(section_date_))[0][1]:
                    Section_date = p_obj.date_parser(eval(str(section_date_))[
                                                     0][0] + " " + eval(str(section_date_))[0][1], (self.yr_, 1, 1))
                    section_dates_ls.append([df.iloc[idx]['Page'], df.iloc[idx]['score'], df.iloc[idx]['entity'], df.iloc[idx]
                                            ['section_entity'], df.iloc[idx]['main_section'], df.iloc[idx]['updated_main_section'], Section_date])
                    df.at[idx, 'date/time'] = str(section_date_)
                    df.at[idx, 'post_process_date/time'] = str(Section_date)
                else:
                    Section_date = p_obj.date_parser(
                        eval(str(section_date_))[0][0], (self.yr_, 1, 1))
                    section_dates_ls.append([df.iloc[idx]['Page'], df.iloc[idx]['score'], df.iloc[idx]['entity'], df.iloc[idx]
                                            ['section_entity'], df.iloc[idx]['main_section'], df.iloc[idx]['updated_main_section'], Section_date])
                    df.at[idx, 'date/time'] = str(section_date_)
                    df.at[idx, 'post_process_date/time'] = str(Section_date)
            else:
                nearest_section_end_idx = lookup_section_end[lookup_section_end < idx][-1] if not lookup_section_end.empty and not lookup_section_end[lookup_section_end < idx].empty else max(1, idx - 10)
                nearest_section_idx = lookup_section_start[lookup_section_start < idx][-1] if not lookup_section_start.empty and not lookup_section_start[lookup_section_start < idx].empty else max(1, idx - 10)
                idx_above = max(nearest_section_end_idx, nearest_section_idx)
                range1 = idx_above - idx if df.iloc[idx_above]['Page'] in [i for i in page_list if i <= df.iloc[idx]['Page']][-2:] else idx - 10

                # lower_limit = max(1, df.iloc[idx]['updated_line_number']-5)
                # upper_limit = min(df.iloc[idx]['updated_line_number']+1, df[df['Page'] == df.iloc[idx]['Page']].iloc[-1]['updated_line_number'])
                # number_range = range(lower_limit, upper_limit)
                # temp_df = df[df['updated_line_number'].isin(number_range) & (df['Page'] == df.iloc[idx]['Page'])][['Text','updated_line_number','section_entity']]
                # if not(temp_df[temp_df['section_entity'] == 'SECTION END'].shape[0]):
                #     range1 = temp_df.index[0] - idx
                # else:
                #     line_number = temp_df[temp_df['section_entity'] == 'SECTION END'].iloc[-1]['updated_line_number']
                #     try:
                #         range1 = temp_df[temp_df['updated_line_number'] == line_number + 1].index[0] - idx
                #     except:
                #         range1 = 0
                # try:
                #     near_se_idx = lookup_section_end[lookup_section_end<idx][-1]
                # except:
                #     near_se_idx = -5
                # range1 = max(-5, near_se_idx-idx)
                section_date__ = self.get_result(
                    df, idx, range1, 0, self.suppress_date_tag_section)
                section_date_ = self.relevant_date_tags(section_date__,self.section_date_tags)
                if len(section_date_) > 0:
                    if eval(str(section_date_))[0][1]:
                        Section_date = p_obj.date_parser(eval(str(section_date_))[
                                                        0][0] + " " + eval(str(section_date_))[0][1], (self.yr_, 1, 1))
                        section_dates_ls.append([df.iloc[idx]['Page'], df.iloc[idx]['score'], df.iloc[idx]['entity'], df.iloc[idx]
                                                ['section_entity'], df.iloc[idx]['main_section'], df.iloc[idx]['updated_main_section'], Section_date])
                        df.at[idx, 'date/time'] = str(section_date_)
                        df.at[idx, 'post_process_date/time'] = str(Section_date)
                    else:
                        Section_date = p_obj.date_parser(
                            eval(str(section_date_))[0][0], (self.yr_, 1, 1))
                        section_dates_ls.append([df.iloc[idx]['Page'], df.iloc[idx]['score'], df.iloc[idx]['entity'], df.iloc[idx]
                                                ['section_entity'], df.iloc[idx]['main_section'], df.iloc[idx]['updated_main_section'], Section_date])
                        df.at[idx, 'date/time'] = str(section_date_)
                        df.at[idx, 'post_process_date/time'] = str(Section_date)
                else:
                    section_dates_ls.append([df.iloc[idx]['Page'], df.iloc[idx]['score'], df.iloc[idx]['entity'], df.iloc[idx]
                                        ['section_entity'], df.iloc[idx]['main_section'], df.iloc[idx]['updated_main_section']])

        try:
            self.sections_df = pd.DataFrame(section_dates_ls)
            self.sections_df.rename(columns={0: 'page_number', 1: 'score', 2: 'entity', 3: 'section_entity',
                                    4: 'main_section', 5: 'updated_main_section', 6: 'dates'}, inplace=True)
        except:
            logging.error('Sections not detected')
            pass

    def update_section_end_date(self, section_end_date_, section_end_date, df, df_end, i, j):
        """
        ### Update the Section End Date in df and df_end.
        args:
            section_end_date_: date/time before Post Process.
            section_end_date: post_process_date/time.
            df: Original DataFrame.
            df_end: Section End DataFrame.
            i: index w.r.t df.
            j: index w.r.t df_end.
        return:
            None
        """
        df_end.at[j, 'date/time'] = str(section_end_date_)
        df.at[i, 'date/time'] = str(section_end_date_)
        df_end.at[j, 'post_process_date/time'] = str(section_end_date)
        df.at[i, 'post_process_date/time'] = str(section_end_date)

    def get_dict(self, df_date):
        # print("rerceived")
        # d = defaultdict(list)
        d = {}
        for row in df_date.values.tolist():
            d[row[1]] = row[2]
        return dict(d)
        
    def get_lab_dates(self, df):
        pattern = "|".join(self.date_tags_regex_dict.values())
        df['test'] = df['updated_main_section'].shift(-1, axis = 0)
        df['test'] = df['test'].ffill()
        df['indicator'] = df['test'] != df['updated_main_section']
        idx = df[df['indicator'] == True].index
        r = df[df['test'] != df['updated_main_section']].shape[0]+1
        df.loc[idx, 'rank'] = list(range(1, r))
        df['rank'] = df['rank'].bfill()
        idx_ = df[df['rank'].isna()].index
        try:
            rank = df['rank'].max() + 1
        except:
            rank = 1
        if len(idx_) > 0:
            df.loc[idx_, 'rank'] = rank
        df_co_max_temp = df.groupby(['rank'])['Page'].max().reset_index()
        df_co_min_temp = df.groupby(['rank'])['Page'].min().reset_index()
        df_co_max_temp.rename(columns = {'Page':'max_page'}, inplace = True)
        df_co_min_temp.rename(columns = {'Page':'min_page'}, inplace = True)
        df = df.merge(df_co_min_temp, on=['rank'], how = 'left')
        df = df.merge(df_co_max_temp, on=['rank'], how = 'left')
        df.loc[:,['min_page_temp']] = df['min_page'].astype('str')
        df.loc[:,['max_page_temp']] = df['max_page'].astype('str')
        df.loc[:,['range']] = "("+df['min_page_temp'] + "," + df['max_page_temp'] + ")"
        
        columns = ['index','Page', 'Geometry.BoundingBox.Width', 'Geometry.BoundingBox.Height', 'Geometry.BoundingBox.Left', 'Geometry.BoundingBox.Top','LINE_NUMBER', 'Text', 'section', 'main_section', 'detected_pattern','detected_date', 'date_info_dict']
        final_result = pd.DataFrame(columns = columns)
        is_rank_selected = []
        for i in df[df['updated_main_section'] == 'Laboratory']['range'].unique().tolist():
            start_page = eval(i)[0]
            end_page = eval(i)[1]
            # print(eval(i)[0], eval(i)[1])
            start_page_rank = df[(df['updated_main_section'] == 'Laboratory') & (df['Page'] == start_page) & (~df['rank'].isin(is_rank_selected))]['rank'].iloc[0]
            is_rank_selected.append(start_page_rank)
            df_labs = df[(df['updated_main_section'] == 'Laboratory') & (df['Page'].isin(range(start_page, end_page+1))) & (df['rank'] == start_page_rank)].copy()
            start_len = df_labs.iloc[0]['Start']
            corpus = self.get_corpus(df_labs)
            ls = []
            match_obj = re.finditer(rf"\b({pattern}|testing performed by)\b", corpus.lower())
            for each_match in match_obj:
                meta_data_df = df_labs[(df_labs['Start'] <= each_match.start() + start_len)
                                        & (df_labs['End'] >= each_match.end() + start_len-1)]
                detected_text = each_match.string[each_match.start():each_match.end()]
                if meta_data_df.shape[0] == 0:
                    meta_data_df = df_labs[(df_labs['Start'] <= each_match.start() + start_len)]
                    d = dict(zip(meta_data_df.columns.to_list(), meta_data_df.values[-1]))
                else:
                    d = dict(zip(meta_data_df.columns.to_list(), meta_data_df.values[0]))
                page = d['Page']
                bb_header_top = d['Geometry.BoundingBox.Top']
                bb_header_left = d['Geometry.BoundingBox.Left']
                bb_header_width = d['Geometry.BoundingBox.Width']
                bb_header_height = d['Geometry.BoundingBox.Height']
                index = d['index']
                bb_header_left -= 0.05
                try:
                    idx = df_labs[(df_labs['Page'] == page) & (df_labs['Geometry.BoundingBox.Top'] > bb_header_top) & (df_labs['Geometry.BoundingBox.Left'] > bb_header_left)].index[0]
                except:
                    continue            
                text = re.sub(r"\,|\-",' ',d['Text'].lower() +" "+ df.iloc[idx]['Text'].lower())
                text = re.sub(r' +', ' ',text)
                text = re.sub(self.phy_name_regex,'',text.lower())
                detected_date_ = DatetimeExtractor.get_date_time_from_corpus_v2(text, self.date_tags)
                detected_date = self.suppress_date(detected_date_)
                date_info_dict = {"Geometry.BoundingBox.Top":bb_header_top, "date":detected_date[0]} if str(detected_date) != '[]' else "{}"
                ls.append([index, page, bb_header_width, bb_header_height, bb_header_left, bb_header_top, d['LINE_NUMBER'], text, d['section'], d['updated_main_section'] ,detected_text,detected_date, date_info_dict])
                # print("Page: ",d['Page'], detected_date)

            result = pd.DataFrame(ls, columns=columns)
            result['detected_date'] = result['detected_date'].astype('str')
            result.drop_duplicates(subset = ['index', 'detected_date'], inplace=True)
            # v1
            lvl = result[result['detected_pattern'] == 'testing performed by'].shape[0]
            idx = result[result['detected_pattern'] == 'testing performed by'].index
            result.loc[idx, 'lab_rank'] = list(range(1,lvl+1))
            result['lab_rank'] = result['lab_rank'].bfill()
            idx_nan = result[result['lab_rank'].isna()].index
            assign_rank = result['lab_rank'].max() + 1 if str(result['lab_rank'].max()) != 'nan' else 1
            result.loc[idx_nan, 'lab_rank'] = assign_rank
            df_date = result[result['date_info_dict'] != '{}'].groupby(['lab_rank', 'Page'])['date_info_dict'].apply(list).rename('lab_date').reset_index()
            for i in df_date['lab_rank'].unique():
                d = self.get_dict(df_date[df_date['lab_rank'] == i])
                df_date.loc[df_date['lab_rank'] == i, 'lab_date_dict'] = str(d)
            if 'lab_date_dict' not in df_date.columns:
                df_date['lab_date_dict'] = None
            df_date.drop_duplicates(subset = ['lab_rank'], keep = 'first', inplace = True)
            result = result.merge(df_date, on=['lab_rank'], how='left')
            df_labs = df_labs.merge(result[['index', 'lab_rank', 'lab_date_dict']], on=['index'], how = 'left')
            for i in df_labs['lab_rank'].value_counts().index:
                if str(i) != 'nan':
                    # print(i, df_labs[df_labs['lab_rank'] == i]['index'].min(), df_labs[df_labs['lab_rank'] == i]['index'].max())
                    start = df_labs[df_labs['lab_rank'] == i]['index'].min()
                    end = df_labs[df_labs['lab_rank'] == i]['index'].max()
                    df_labs.loc[df_labs['index'].isin(list(range(start, end+1))), 'lab_date_dict'] = df_labs.loc[df_labs['index'].isin(list(range(start, end+1))), 'lab_date_dict'].ffill()
            final_result = pd.concat([final_result, df_labs])
        final_result = final_result.drop_duplicates(subset='index', keep='first')
        if not final_result.empty:
            df  = df.merge(final_result[['index', 'lab_date_dict']], on=['index'], how='left')
        else:
            df['lab_date_dict'] = str({})
        df.drop(columns= {'test', 'indicator', 'min_page', 'max_page', 'min_page_temp', 'max_page_temp', 'range', 'rank'}, inplace=True)
        return df
 
    def section_end_info(self, df):
        """
        ### Extract the Physician Name, Creds, Dates from the Section End.
        args:
            df: DataFrame
        return:
            None
        """
        # ----------------------------- Section End -> Physician Name, Date ---------------------------
        logging.info('Extracting Physician Name, Date')
        df['Physician Name'] = None
        section_end_start_pattern_regex = "|".join(self.section_end_start_pattern)
        creds_regex = "|".join(self.creds_pattern)
        section_end_end_pattern_regex = "|".join(self.section_end_end_pattern)
        crp = self.get_corpus(df)
        p_obj = Postprocess(self.adm_dis_tag)
        self.yr__ = p_obj.get_adm_discharge_date(crp)
        j = 0
        df_end = pd.DataFrame()
        pattern1 = rf"(?:{section_end_start_pattern_regex})[^A-Za-z0-9]*(.*?)(?:{section_end_end_pattern_regex})|(?:{section_end_start_pattern_regex})[^A-Za-z0-9]*(.*)(?:{section_end_end_pattern_regex})?"
        for i in df[df['section_entity'] == "SECTION END"].index.to_list():
            text = " ".join(df.iloc[i:i+1]['Text'])
            df_end.at[j, 'Page'] = int(df.iloc[i]['Page'])
            df_end.at[j, 'Index'] = int(i)
            res = ""
            try:
                non_extra_whitespaces_text, _, _ = self.text_cleanup(
                    "".join(re.findall(pattern1, text)[0]).strip())
                if len(non_extra_whitespaces_text) > 0:
                    df_end.at[j, 'Text'] = text
                    df_end.at[j, 'Physician Name'] = "".join(
                        re.findall(pattern1, text)[0]).strip().upper()
                    df.at[i, 'Physician Name'] = "".join(
                        re.findall(pattern1, text)[0]).strip().upper()  # up
                else:
                    text = " ".join(df.iloc[i:i+2]['Text'])
                    if len("".join(re.findall(pattern1, text)[0]).strip()) > 0:
                        df_end.at[j, 'Text'] = text
                        df_end.at[j, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()
                        df.at[i, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()  # up
            except:
                text = " ".join(df.iloc[i:i+2]['Text'])
                try:
                    if len("".join(re.findall(pattern1, text)[0]).strip()) > 0:
                        df_end.at[j, 'Text'] = text
                        df_end.at[j, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()
                        df.at[i, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()  # up
                    elif len("".join(re.findall(pattern1, text.lower())[0]).strip()) > 0:
                        text = text.lower()
                        df_end.at[j, 'Text'] = text
                        df_end.at[j, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()
                        df.at[i, 'Physician Name'] = "".join(
                            re.findall(pattern1, text)[0]).strip().upper()
                except:
                    text = " ".join(df.iloc[i:i+2]['Text']).lower()
                    try:
                        if len("".join(re.findall(pattern1, text)[0]).strip()) > 0:
                            df_end.at[j, 'Text'] = text
                            df_end.at[j, 'Physician Name'] = "".join(
                                re.findall(pattern1, text)[0]).strip().upper()
                            df.at[i, 'Physician Name'] = "".join(
                                re.findall(pattern1, text)[0]).strip().upper()
                    except:
                        pass
                    # try:
                    #     print(i, df_end.at[j, 'Physician Name'])
                    # except KeyError as e:
                    #     pass

            # Date Logic
            section_end_date_ = self.get_result(
                df, i, 0, 4, self.suppress_date_tag_section_end)
            section_end_date_ = self.relevant_date_tags(section_end_date_,self.section_end_date_tags)
            if len(section_end_date_) != 0:
                if eval(str(section_end_date_))[0][1]:
                    section_end_date = p_obj.date_parser(eval(str(section_end_date_))[
                                                         0][0] + " " + eval(str(section_end_date_))[0][1], (self.yr__, 1, 1))
                    self.update_section_end_date(
                        section_end_date_, section_end_date, df, df_end, i, j)
                else:
                    section_end_date = p_obj.date_parser(
                        eval(str(section_end_date_))[0][0], (self.yr__, 1, 1))
                    self.update_section_end_date(
                        section_end_date_, section_end_date, df, df_end, i, j)
            else:
                section_end_date_ = self.get_result(
                    df, i, -2, 0, self.suppress_date_tag_section_end)
                section_end_date_ = self.relevant_date_tags(section_end_date_,self.section_end_date_tags)
                if len(section_end_date_) != 0:
                    if eval(str(section_end_date_))[0][1]:
                        section_end_date = p_obj.date_parser(eval(str(section_end_date_))[
                                                             0][0] + " " + eval(str(section_end_date_))[0][1], (self.yr__, 1, 1))
                        self.update_section_end_date(
                            section_end_date_, section_end_date, df, df_end, i, j)

                    else:
                        section_end_date = p_obj.date_parser(
                            eval(str(section_end_date_))[0][0], (self.yr__, 1, 1))
                        self.update_section_end_date(
                            section_end_date_, section_end_date, df, df_end, i, j)
            j += 1

        PATTERN_CREDS = rf"\b({creds_regex})\b"
        df_end = df_end.reset_index()
        df_end.drop(columns={'index'}, inplace=True)
        for i in range(len(df_end)):
            try:
                if ",".join(re.findall(PATTERN_CREDS, df_end.iloc[i]['Physician Name'])).strip():
                    df_end.at[i, 'physician_credential'] = ",".join(re.findall(
                        PATTERN_CREDS, df_end.iloc[i]['Physician Name'])).strip()
                else:
                    df_end.at[i, 'physician_credential'] = ",".join(re.findall(
                        PATTERN_CREDS, df_end.iloc[i]['Text'])).strip()
            except:
                pass
        section_end_index = df[df['section_entity']
                               == 'SECTION END'].index  # up
        for i in section_end_index:
            try:
                if ",".join(re.findall(PATTERN_CREDS, df.iloc[i]['Physician Name'])).strip():
                    df.at[i, 'physician_credential'] = ",".join(re.findall(
                        PATTERN_CREDS, df.iloc[i]['Physician Name'])).strip()
                else:
                    df.at[i, 'physician_credential'] = ",".join(re.findall(
                        PATTERN_CREDS, df.iloc[i]['Text'])).strip()
            except:
                pass  # up

        columns_ls_df_end = ['Page', 'Index', 'Text', 'Physician Name', 'date/time', 'post_process_date/time', 'physician_credential']
        for i in columns_ls_df_end:
            if i not in df_end.columns.to_list():
                df_end[i] = None

        columns_ls_df = ['Physician Name', 'date/time', 'post_process_date/time', 'physician_credential']
        for i in columns_ls_df:
            if i not in df.columns.to_list():
                df[i] = None
        self.df_end = df_end

    def group_anagrams(self, words):
        anagram_dict = {}
        for index, word in enumerate(words):
            cleaned_word = re.sub(r"[^A-Za-z0-9]", "", word).lower()
            sorted_word = tuple(sorted(cleaned_word))
            if sorted_word not in anagram_dict:
                anagram_dict[sorted_word] = []
            anagram_dict[sorted_word].append(word)
        return list(anagram_dict.values())

    def remove_subsets(self, words):
        cleaned_words = [''.join(re.findall(r'[A-Za-z]', word)).lower() for word in words]
        paired_words = sorted(zip(words, cleaned_words), key = lambda x: len(x[1]), reverse=True)
        result = []
        seen = set()
        for original_word, cleaned_word in paired_words:
            if not any(cleaned_word in s for s in seen):
                result.append(original_word)
                seen.add(cleaned_word)
        return result

    def get_unique_names(self, words):
        result = self.group_anagrams(words)

        res = []
        for i in result:
            res.append(i[0])
            
        final_res = self.remove_subsets(res)
        final_res = sorted(final_res, key=len, reverse=False)
        return final_res
    
    def get_chart_order_0(self, df):
        req_ls = ['index','Page', 'entity', 'score', 'section_entity', 'sub_section_entity', 'updated_main_section', 'post_process_date/time', 'Physician Name', 'physician_credential', 'updated_is_section_end_present']
        df_co = df[[i for i in req_ls if i in df.columns]]
        df_co = df_co[(df_co['updated_is_section_end_present'].isin([0, 3]))]
        df_co.rename(columns = {'updated_main_section':'main_section'}, inplace = True)
        df_co = df_co.dropna(subset = 'main_section')
        df_co['test'] = df_co['entity'].shift(-1, axis = 0)
        df_co['test'].fillna(method='ffill', inplace=True)
        df_co['indicator'] = df_co['test'] != df_co['entity']
        idx = df_co[df_co['indicator'] == True].index
        r = df_co[df_co['test'] != df_co['entity']].shape[0]+1
        df_co.loc[idx, 'rank'] = list(range(1, r))
        df_co['rank'].fillna(method = 'bfill', inplace = True)
        idx = df_co[df_co['rank'].isna()].index
        rank = df_co['rank'].max() + 1 if str(df_co['rank'].max()).lower() not in ['nan', 'none'] else 1
        df_co.loc[idx, 'rank'] = rank

        df_co_max_temp = df_co.groupby(['rank', 'entity'])['Page'].max().reset_index()
        df_co_min_temp = df_co.groupby(['rank', 'entity'])['Page'].min().reset_index()
        df_co_max_temp.rename(columns = {'Page':'max_page'}, inplace = True)
        df_co_min_temp.rename(columns = {'Page':'min_page'}, inplace = True)
        df_co = df_co.merge(df_co_min_temp, on=['rank', 'entity'], how = 'left')
        df_co = df_co.merge(df_co_max_temp, on=['rank', 'entity'], how = 'left')
        df_co['post_process_date/time'] = pd.to_datetime(df_co['post_process_date/time'], errors='coerce')
        d2 = df_co.drop_duplicates(subset= 'rank', keep='last')
        d2['min_page_temp'] = d2['min_page'].astype('str')
        d2['max_page_temp'] = d2['max_page'].astype('str')
        d2['range'] = "("+d2['min_page_temp'] + "," + d2['max_page_temp'] + ")"
        d2.rename(columns = {'physician_credential':'standard_creds'}, inplace = True)
        d2.drop(columns = ['min_page_temp', 'max_page_temp', 'indicator', 'test'], inplace = True)
        d2['entity_actual'] = d2['entity']
        d2['main_section_actual'] = d2['main_section']
        df_date = df_co.sort_values(['rank','post_process_date/time'], ascending = [True, True]).drop_duplicates(subset= 'rank', keep='first')
        df_date.rename(columns = {'post_process_date/time':'group_date'}, inplace = True)
        self.df_co_0 = d2.merge(df_date[['group_date', 'rank']], on=['rank'], how='left')
        self.df_co_0['file_name'] = self.file_name.replace('.csv', '')

    def get_chart_order_0_v1(self, df):
        req_ls = ['index','Page', 'entity', 'score', 'section_entity', 'sub_section_entity', 'updated_main_section', 'post_process_date/time', 'Physician Name', 'physician_credential', 'updated_is_section_end_present']
        df_co = df[[i for i in req_ls if i in df.columns]]
        if not df[df['section_entity'] == 'SECTION'].index.empty:
            df_co = df_co[(df_co['updated_is_section_end_present'].isin([0, 3, 1]))]
            df_co.rename(columns = {'updated_main_section':'main_section'}, inplace = True)
            df_co = df_co.dropna(subset = 'main_section')
            df_co['test'] = df_co['entity'].shift(-1, axis = 0)
            df_co['test'].fillna(method='ffill', inplace=True)
            df_co['indicator'] = df_co['test'] != df_co['entity']
            idx = df_co[df_co['indicator'] == True].index
            r = df_co[df_co['test'] != df_co['entity']].shape[0]+1
            df_co.loc[idx, 'rank'] = list(range(1, r))
            df_co['rank'].fillna(method = 'bfill', inplace = True)
            idx = df_co[df_co['rank'].isna()].index
            rank = df_co['rank'].max() + 1 if str(df_co['rank'].max()).lower() not in ['nan', 'none'] else 1
            df_co.loc[idx, 'rank'] = rank

            df_co_max_temp = df_co.groupby(['rank', 'entity', 'updated_is_section_end_present'])['Page'].max().reset_index()
            df_co_min_temp = df_co.groupby(['rank', 'entity', 'updated_is_section_end_present'])['Page'].min().reset_index()
            df_co_max_temp.rename(columns = {'Page':'max_page'}, inplace = True)
            df_co_min_temp.rename(columns = {'Page':'min_page'}, inplace = True)
            df_co = df_co.merge(df_co_min_temp, on=['rank', 'entity', 'updated_is_section_end_present'], how = 'left')
            df_co = df_co.merge(df_co_max_temp, on=['rank', 'entity', 'updated_is_section_end_present'], how = 'left')

            df_co['post_process_date/time'] = pd.to_datetime(df_co['post_process_date/time'], errors='coerce')
            d2 = df_co.drop_duplicates(subset= ['rank', 'updated_is_section_end_present'], keep='last')
            d2.loc[:,['min_page_temp']] = d2['min_page'].astype('str')
            d2.loc[:,['max_page_temp']] = d2['max_page'].astype('str')
            d2.loc[:,['range']] = "("+d2['min_page_temp'] + "," + d2['max_page_temp'] + ")"
            d2.rename(columns = {'physician_credential':'standard_creds'}, inplace = True)
            d2.drop(columns = ['min_page_temp', 'max_page_temp', 'indicator', 'test'], inplace = True)
            d2.loc[:,['entity_actual']] = d2['entity']
            d2.loc[:,['main_section_actual']] = d2['main_section']
            df_date = df_co.sort_values(['rank','post_process_date/time'], ascending = [True, True]).drop_duplicates(subset= 'rank', keep='first')
            df_date.rename(columns = {'post_process_date/time':'group_date'}, inplace = True)
            self.df_co_0 = d2.merge(df_date[['group_date', 'rank']], on=['rank'], how='left')
            self.df_co_0.loc[:,['file_name']] = self.file_name.replace('.csv', '')
            self.df_co_0 = self.df_co_0[(self.df_co_0['updated_is_section_end_present'].isin([0, 3]))]
        else:
            self.df_co_0 = pd.DataFrame({})

    def chart_order_logic(self, df):
        """
        ### Chart Order for the DataFrame.
        args:
            df: DataFrame
        return:
            None
        """
        # ----------------------------------------- Chart Order----------------------------------------
        logging.info('Chart Order')
        # df_co = df[(df['section_entity'] == "SECTION") | (
        #     df['section_entity'] == "SUB SECTION") | (df['section_entity'] == "SECTION END")]
        # df_co = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (df['section_end_pattern'] != 'performed by') & (df['updated_is_section_end_present'].isin([1, 1.0]))] 
        if not df[df['section_entity'] == 'SECTION'].index.empty:
            df_co = df[(df['section_entity'].isin(['SECTION', 'SECTION END'])) & (~df['section_end_pattern'].isin(self.suppress_section_end_pattern)) & (df['updated_is_section_end_present'].isin([1, 1.0]))] 
            req_ls = ['index','Page', 'entity', 'score', 'section_entity', 'sub_section_entity', 'updated_main_section', 'post_process_date/time', 'Physician Name', 'physician_credential', 'updated_is_section_end_present']
            df_co = df_co[[i for i in req_ls if i in df.columns]]
            df_co.rename(
                columns={"updated_main_section": "main_section"}, inplace=True)
            df_co = df_co.reset_index()

            count, flag = 1, 0
            df_co['group'] = np.NaN
            for i in range(len(df_co)):
                if (df_co.iloc[i]['section_entity'] == "SECTION" or df_co.iloc[i]['section_entity'] == "SUB SECTION") and flag == 0:
                    flag = 0
                    continue
                if df_co.iloc[i]['section_entity'] == "SECTION END":
                    flag = 1
                    df_co.at[i, 'group'] = f'Group{count}'
                if df_co.iloc[i]['section_entity'] == "SECTION" or df_co.iloc[i]['section_entity'] == "SUB SECTION" and flag == 1:
                    flag = 0
                    count += 1

            df_co['group'] = df_co['group'].replace(np.NaN, 'nan')
            # If there is no Section End present at last, we need to consider it as one group
            if (not df_co.empty) and (df_co.iloc[len(df_co)-1]['group'] == None or df_co.iloc[len(df_co)-1]['group'] == 'nan' or df_co.iloc[len(df_co)-1]['group'] == np.NaN):
                df_co.at[len(df_co)-1, 'group'] = f'Group{count}'

            df_co['group'] = df_co['group'].replace(np.NaN, 'nan')
            df_co['group'] = df_co['group'].mask(
                df_co['group'] == 'nan', None).bfill()

            for i in list(set(df_co['group'].to_list())):
                index = df_co[df_co['group'] == i].index[-1]
                idx = df_co[df_co['group'] == i].index
                df_co.at[index, 'range'] = str((df_co[df_co['group'] == i].iloc[0]['Page'], df_co[df_co['group'] == i].iloc[-1]['Page']))
                df_co.loc[idx, 'min_page'] = df_co[df_co['group'] == i].iloc[0]['Page']
                df_co.loc[idx, 'max_page'] = df_co[df_co['group'] == i].iloc[-1]['Page']

            # If we're fiding a SUB SECTION after SECTION END, we're suppressing them !
            # df_co['range'] = df_co['range'].replace(np.NaN, 'nan')
            # keys = df_co[df_co['range'] != 'nan'].index.to_list()
            # values = df_co[df_co['range'] != 'nan']['range'].to_list()
            # pairs = zip(keys, values)
            # d = dict(pairs)

            # for k, v in d.items():
            #     try:
            #         if df_co.iloc[k+1]['section_entity'] == "SUB SECTION":
            #             grp_value = df_co.iloc[k+1]['group']
            #             main_section_name_index = df_co[df_co['group']
            #                                             == grp_value].index
            #             df_co.loc[main_section_name_index,
            #                       'main_section'] = df_co.iloc[k]['main_section']
            #             df_co['group'] = df_co['group'].replace(grp_value, 'nan')
            #             df_co.at[k, 'range'] = 'nan'
            #     except:
            #         pass

            # df_co['group'] = df_co['group'].mask(
            #     df_co['group'] == 'nan', None).ffill()
            # df_co['range'] = 'nan'
            # grp_set = set(df_co['group'].to_list())
            # for i in grp_set:
            #     index = df_co[df_co['group'] == i].index[-1]
            #     df_co.at[index, 'range'] = str(
            #         (df_co[df_co['group'] == i].iloc[0]['Page'], df_co[df_co['group'] == i].iloc[-1]['Page']))

            # If we're fiding a SECTION and SECTION END in a same page, we're suppressing them ! (adding them to the previous page.)
            if 'range' not in df_co.columns:
                df_co['range'] = np.nan
            # we stopped at here
            if not self.df_co_0.empty:
                try:
                    self.df_co_0.drop(columns = {'level_0'}, inplace = True)
                except:
                    pass
                try:
                    df_co.drop(columns = {'level_0'}, inplace = True)
                except:
                    pass
                df_co = pd.concat([df_co, self.df_co_0])
                df_co.sort_values(['index'], inplace = True)
                df_co = df_co.reset_index()
                df_co['group'] = df_co['group'].apply(lambda x : str(x).lower())
                df_co.loc[df_co[df_co['updated_is_section_end_present'].isin([0, 3])].index, 'post_process_date/time'] = df_co[df_co['updated_is_section_end_present'].isin([0, 3])]['group_date'].tolist()
                df_co.loc[df_co[df_co['updated_is_section_end_present'].isin([0, 3])].index, 'entity'] = df_co[df_co['updated_is_section_end_present'].isin([0, 3])]['entity_actual'].tolist()
                df_co.loc[df_co[df_co['updated_is_section_end_present'].isin([0, 3])].index, 'main_section'] = df_co[df_co['updated_is_section_end_present'].isin([0, 3])]['main_section_actual'].tolist()
                df_co.drop(columns={'group_date'}, inplace=True)
                df_co.drop(columns={'file_name'}, inplace=True)
                df_co.rename(columns={'rank':'rank_'}, inplace=True)
                try:
                    df_co.drop(columns = {'level_0'}, inplace = True)
                except:
                    pass
                for idx in df_co[df_co['updated_is_section_end_present'].isin([0, 3])].index:
                    df_co.at[idx, 'group'] = f"group{int(count+df_co.iloc[idx]['rank_'])}"
            df_co['range'] = df_co['range'].apply(lambda x : str(x).lower())
            keys = df_co[df_co['range'] != 'nan'].index.to_list()
            values = df_co[df_co['range'] != 'nan']['range'].to_list()
            pairs = zip(keys, values)
            d = dict(pairs)
            prev_key = None
            for k, v in d.items():
                try:
                    if abs(eval(v)[0] - eval(v)[1]) == 0 and prev_key != None and df_co.iloc[prev_key]['main_section'] == df_co.iloc[k]['main_section']:
                        df_co['group'] = df_co['group'].replace(df_co.iloc[k]['group'], 'nan')
                except:
                    pass
                prev_key = k
            df_co['group'] = df_co['group'].mask(df_co['group'] == 'nan', None).ffill()
            if not df_co[df_co['group'].isna()].empty:
                df_co.loc[df_co[df_co['group'].isna()].index, 'group'] = 'group' + str(int(min(df_co['group'].value_counts().index.to_list()).split('group')[1])-1)

            df_min_page = df_co.groupby(['group'])['min_page'].min().reset_index()
            df_max_page = df_co.groupby(['group'])['max_page'].max().reset_index()
            df_co.drop(columns = {'min_page', 'max_page'}, inplace = True)
            df_co = df_co.merge(df_min_page[['group', 'min_page']], on=['group'], how = 'left')
            df_co = df_co.merge(df_max_page[['group', 'max_page']], on=['group'], how = 'left')
            df_co['range'] = 'nan'
            
            grp_set = set(df_co['group'].to_list())
            for i in grp_set:
                index = df_co[df_co['group'] == i].index[-1]
                # df_co.at[index, 'range'] = str((df_co[df_co['group'] == i].iloc[0]['Page'], df_co[df_co['group'] == i].iloc[-1]['Page']))
                df_co.at[index, 'range'] = str((df_co[df_co['group'] == i].iloc[0]['min_page'], df_co[df_co['group'] == i].iloc[0]['max_page']))

            # If we're fiding a SECTION and SECTION END in a same page, we're suppressing them ! (adding them to the next page.)
            keys = df_co[df_co['range'] != 'nan'].index.to_list()
            values = df_co[df_co['range'] != 'nan']['range'].to_list()
            pairs = zip(keys, values)
            d = dict(pairs)
            for k, v in d.items():
                try:
                    if abs(eval(v)[0] - eval(v)[1]) == 0 and df_co.iloc[k+1]['main_section'] == df_co.iloc[k]['main_section'] and df_co.iloc[k+1]['entity'] == df_co.iloc[k]['entity']:
                        df_co['group'] = df_co['group'].replace(
                            df_co.iloc[k+1]['group'], 'nan')
                except:
                    pass

            df_co['group'] = df_co['group'].mask(df_co['group'] == 'nan', None).ffill()
            df_min_page = df_co.groupby(['group'])['min_page'].min().reset_index()
            df_max_page = df_co.groupby(['group'])['max_page'].max().reset_index()
            df_co.drop(columns = {'min_page', 'max_page'}, inplace = True)
            df_co = df_co.merge(df_min_page[['group', 'min_page']], on=['group'], how = 'left')
            df_co = df_co.merge(df_max_page[['group', 'max_page']], on=['group'], how = 'left')

            df_co['range'] = 'nan'
            grp_set = set(df_co['group'].to_list())
            df_co['group_date'] = ''
            df_co['_Score_'] = None
            for i in grp_set:
                index = df_co[df_co['group'] == i].index[-1]
                df_co.at[index, 'range'] = str(
                    (df_co[df_co['group'] == i].iloc[0]['min_page'], df_co[df_co['group'] == i].iloc[0]['max_page']))
                try:
                    try:
                        df_co.at[index, 'group_date'] = min([k for k in df_co[(df_co['group'] == i) & (
                            df_co['section_entity'] != "SECTION END")]['post_process_date/time'].to_list() if str(k) != 'nan' and str(k).lower() != 'none'])
                    except:
                        df_co.at[index, 'group_date'] = max([k for k in df_co[(df_co['group'] == i) & (
                            df_co['section_entity'] == "SECTION END")]['post_process_date/time'].to_list() if str(k) != 'nan' and str(k).lower() != 'none'])

                except:
                    pass
                try:
                    df_co.at[index, '_Score_'] = df_co[(df_co['group'] == i) & (
                        df_co['section_entity'] == 'SECTION')].iloc[0]['score']
                except:
                    pass

            df_co['group_date'] = pd.to_datetime(
                df_co['group_date'], errors='coerce')
            # df_co.drop(columns={'Date'},inplace=True)
            df_co['file_name'] = self.file_name.replace('.csv', '')
            phy_creds = ['MD', 'DO', 'DPM', 'PA', 'PA-C', 'CRNA',
                        'RN', 'D.O.', 'PHD', 'MBBS', 'NP', 'APRN', 'DNP-APRN']
            df_co['PHYSICIAN NAME'] = None
            df_co['main_section_actual'] = None
            df_co['entity_actual'] = None
            grp_set = set(df_co['group'].to_list())
            df_co['standard_creds'] = df_co['physician_credential'].map({value: key for key, values in self.creds_dict.items() for value in values})
            df_co['rank'] = df_co['standard_creds'].map(self.creds_rank)
            for i in grp_set:
                index = df_co[df_co['group'] == i].index[-1]
                temp_ = df_co[df_co['group'] == i]
                rank = temp_.sort_values(['rank']).iloc[0]['rank']
                if str(rank).lower() != 'nan':
                    temp = df_co[(df_co['group'] == i) & (df_co['rank'].isin([rank]))]
                    phy_name_ls_ = temp['Physician Name'].unique().tolist()
                    phy_name_ls = self.get_unique_names(phy_name_ls_)
                    df_co.at[index, 'PHYSICIAN NAME'] = str(phy_name_ls)
                    
            grp_set = set(df_co['group'].to_list())
            for i in grp_set:
                idx = df_co[df_co['group'] == i].index[0]
                idx_last = df_co[df_co['group'] == i].index[-1]
                df_co.at[idx_last,'entity_actual'] = df_co.iloc[idx]['entity']
                df_co.at[idx_last,'main_section_actual'] = df_co.iloc[idx]['main_section']

            df_co_date_order = df_co[df_co['range'] !='nan'].sort_values(by=['group_date'])
            df_co_date_order.drop(
                columns={'physician_credential', 'Physician Name'}, inplace=True)
            df_co_date_order.rename(
                columns={'PHYSICIAN NAME': 'Physician Name'}, inplace=True)
            df_co_date_order.drop(columns = {'rank'}, inplace = True)
            self.df_co = df_co
            self.df_co_date_order = df_co_date_order
        else:
            self.df_co = pd.DataFrame({})
            self.df_co_date_order = pd.DataFrame({})

    def concat_files(self):
        print("merging")
        if not self.df_co_date_order.empty:
            try:
                self.df_co_date_order.drop(columns = {'level_0'}, inplace = True)
            except:
                pass
            try:
                self.df_co_0.drop(columns = {'level_0'}, inplace = True)
            except:
                pass
            # df_co_final_result = pd.concat([self.df_co_date_order, self.df_co_0])
            df_co_final_result = self.df_co_date_order
            df_co_final_result.sort_values(['index'], inplace = True)
            self.df_co_final_result = df_co_final_result

            df_co_final_result = df_co_final_result.reset_index()
            df_co_final_result.drop(columns = {'level_0'}, inplace=True)
            for idx, row in df_co_final_result.iterrows():
                df_co_final_result.at[idx, 'grp'] = f'Grp{idx+1}'
            df_co_final_result['min_page'] = df_co_final_result['range'].apply(lambda x: int(eval(x)[0]))
            df_co_final_result['max_page'] = df_co_final_result['range'].apply(lambda x: int(eval(x)[1]))
            df_co_final_result['min_page'] = df_co_final_result['min_page'].astype('int64')
            df_co_final_result['max_page'] = df_co_final_result['max_page'].astype('int64')
            df_co_ = df_co_final_result.copy()
            df_co_['updated_is_section_end_present'] =  df_co_['updated_is_section_end_present'].astype('int64')
            keys = df_co_[df_co_['range'] != 'nan'].index.to_list()
            values = df_co_[df_co_['range'] != 'nan']['range'].to_list()
            pairs = zip(keys, values)
            d = dict(pairs)
            prev_key = None
            for k, v in d.items():
                try:
                    if (abs(eval(v)[0] - eval(v)[1]) <= 1) and (prev_key != None) and (df_co_.iloc[prev_key]['main_section_actual'] == df_co_.iloc[k]['main_section_actual']) and (df_co_.iloc[k]['updated_is_section_end_present'] in [0, 3]) and (df_co_.iloc[prev_key]['section_entity'] != 'SECTION END'):
                    #if (abs(eval(v)[0] - eval(v)[1]) <= 1) and (prev_key != None) and (df_co_.iloc[prev_key]['main_section_actual'] == df_co_.iloc[k]['main_section_actual']) and (df_co_.iloc[k]['updated_is_section_end_present'] == 0) and (df_co_.iloc[prev_key]['section_entity'] != 'SECTION END'):
                        df_co_['grp'] = df_co_['grp'].replace(df_co_.iloc[k]['grp'], 'nan')
                        print(k, v, df_co_.iloc[k]['index'], df_co_.iloc[k]['updated_is_section_end_present'])
                except:
                    pass
                prev_key = k
            if 'grp' not in df_co_.columns:
                df_co_['grp'] = None
                
            df_co_['grp'] = df_co_['grp'].mask(df_co_['grp'] == 'nan', None).ffill()
            df_min_page = df_co_.groupby(['grp'])['min_page'].min().reset_index()
            df_max_page = df_co_.groupby(['grp'])['max_page'].max().reset_index()
            print(df_min_page.shape, df_max_page.shape, df_min_page.columns)
            df_co_.drop(columns = {'min_page', 'max_page'}, inplace = True)
            df_co_ = df_co_.merge(df_min_page[['grp', 'min_page']], on=['grp'], how = 'left')
            df_co_ = df_co_.merge(df_max_page[['grp', 'max_page']], on=['grp'], how = 'left')

            df_co_['min_page_temp'] = df_co_['min_page'].astype('str')
            df_co_['max_page_temp'] = df_co_['max_page'].astype('str')
            df_co_['range'] = np.NaN
            df_co_['range'] = "(" + df_co_['min_page_temp'] + "," + df_co_['max_page_temp'] + ")"

            df_co_['group_date'] = pd.to_datetime(df_co_['group_date'], errors='coerce')
            df_date = df_co_.sort_values(['group_date','grp'], ascending = [True, True]).drop_duplicates(subset= 'grp', keep='first')
            df_date.dropna(subset = 'group_date', inplace = True)
            df_co_.drop(columns = {'group_date'}, inplace = True)
            df_co_ = df_co_.merge(df_date[['grp', 'group_date']], on=['grp'], how = 'left')
            df_co_.drop_duplicates(subset = 'grp', keep = 'last', inplace = True)
            df_co_.sort_values(['index'], inplace = True)
            print(df_co_.shape, "\n\n")

            keys = df_co_[df_co_['range'] != 'nan'].index.to_list()
            values = df_co_[df_co_['range'] != 'nan']['range'].to_list()
            pairs = zip(keys, values)
            d = dict(pairs)
            for k, v in d.items():
                try:
                    if (abs(eval(v)[0] - eval(v)[1]) <= 1) and (df_co_.iloc[k+1]['main_section_actual'] == df_co_.iloc[k]['main_section_actual']) and (df_co_.iloc[k]['updated_is_section_end_present'] in [0, 3]):
                    #if (abs(eval(v)[0] - eval(v)[1]) <= 1) and (df_co_.iloc[k+1]['main_section_actual'] == df_co_.iloc[k]['main_section_actual']) and (df_co_.iloc[k]['updated_is_section_end_present'] == 0):
                        df_co_['grp'] = df_co_['grp'].replace(df_co_.iloc[k+1]['grp'], 'nan')
                        print(k, v, df_co_.iloc[k]['index'], df_co_.iloc[k]['updated_is_section_end_present'])
                except:
                    pass

            df_co_['grp'] = df_co_['grp'].mask(df_co_['grp'] == 'nan', None).ffill()
            df_co_['min_page'] = df_co_['min_page'].astype('int64')
            df_co_['max_page'] = df_co_['max_page'].astype('int64')
            df_min_page = df_co_.groupby(['grp'])['min_page'].min().reset_index()
            df_max_page = df_co_.groupby(['grp'])['max_page'].max().reset_index()
            print(df_min_page.shape, df_max_page.shape, df_min_page.columns)
            df_co_.drop(columns = {'min_page', 'max_page'}, inplace = True)
            df_co_ = df_co_.merge(df_min_page[['grp', 'min_page']], on=['grp'], how = 'left')
            df_co_ = df_co_.merge(df_max_page[['grp', 'max_page']], on=['grp'], how = 'left')
            df_co_['min_page_temp'] = df_co_['min_page'].astype('str')
            df_co_['max_page_temp'] = df_co_['max_page'].astype('str')
            #error
            df_co_['range'] = np.NaN
            df_co_['range'] = "(" + df_co_['min_page_temp'] + "," + df_co_['max_page_temp'] + ")"
            df_co_['group_date'] = pd.to_datetime(df_co_['group_date'], errors='coerce')
            df_date = df_co_.sort_values(['group_date','grp'], ascending = [True, True]).drop_duplicates(subset= 'grp', keep='first')
            df_date.dropna(subset = 'group_date', inplace = True)
            df_co_.drop(columns = {'group_date'}, inplace = True)
            df_co_ = df_co_.merge(df_date[['grp', 'group_date']], on=['grp'], how = 'left')
            df_co_.drop_duplicates(subset = 'grp', keep = 'last', inplace = True)
            df_co_.sort_values(['index'], inplace = True)
            print(df_co_.shape)
            df_co_.drop(columns = {'min_page_temp', 'max_page_temp', 'grp', 'standard_creds', 'sub_section_entity', 'score'}, inplace = True)
            print(df_co_.shape)
            self.df_co_date_order_ = df_co_.copy()
        else:
            cols = ['index', 'Page', 'entity', 'section_entity', 'main_section','post_process_date/time', 'updated_is_section_end_present', 'group',
                    'range', 'rank_', 'entity_actual', 'main_section_actual', '_Score_','file_name', 'Physician Name', 'min_page', 'max_page', 'group_date']
            self.df_co_date_order_ = pd.DataFrame({}, columns=cols)

    def subsection_mapping(self, df):
        temp_dict = self.sub_section_data[['sub_section_name', 'zai_mapped_sub_section']].drop_duplicates().set_index(['sub_section_name']).to_dict()['zai_mapped_sub_section']
        temp_dict1 =dict()
        for key, value in temp_dict.items():
            if str(key).strip() != '' and str(key).lower() != 'nan':
                new_key = key.lower()
                temp_dict1[new_key] = value
        temp_dict2 = {key: val for key, val in sorted(temp_dict1.items(), key = lambda ele: ele[0])}
        subsection_mapping_dict = dict()
        for key, value in temp_dict2.items():
            temp = self.text_cleanup(key)[0]
            subsection_mapping_dict[temp] = value
        df['zai_mapped_sub_section'] = df['sub_section_entity'].copy()
        df['zai_mapped_sub_section'] = df['zai_mapped_sub_section'].apply(lambda x: str(x).lower() if str(x).lower() not in ['nan', 'none'] else x)
        df['section_entity_temp'] = df['section_entity'].copy()
        df['zai_mapped_sub_section'] = df['zai_mapped_sub_section'].mask(df['zai_mapped_sub_section'] == '', None).ffill()
        df['section_entity_temp'] = df['section_entity_temp'].mask(df['section_entity_temp'] == '', None)
        df['section_entity_temp'] = df['section_entity_temp'].ffill()
        df['zai_mapped_sub_section'] = df[df['section_entity_temp'].isin(['SUB SECTION'])]['zai_mapped_sub_section'].ffill()	
        df.loc[df[df['updated_main_section'] == 'Laboratory'].index, 'zai_mapped_sub_section'] = 'Labs'
        df.drop(columns = ['section_entity_temp'], inplace=True)
        df['zai_mapped_sub_section'] = df['zai_mapped_sub_section'].replace(subsection_mapping_dict)
        return df
    
    def section_subsection_algorithm(self, save_path, bucket_name):
        # Configure the logging system
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
        logging.info('FUNCTION START: rev_section_subsection.py')

        csv_textract_obj = self.s3_c.get_object(
            Bucket=self.bucket_name, Key=rf"{self.textract_file}")
        csv_textract_body = csv_textract_obj['Body']
        csv_textract_string = csv_textract_body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_textract_string), na_filter=False)

        # --------------- local ---------------
        # df = pd.read_csv(rf"{self.textract_file}", na_filter=False) 
        # --------------- end ---------------
        print(f"Read: {self.textract_file}")
        print("Number of pages: ", df.iloc[-1]["Page"])
        df = Section.assign_index(df)
        logging.info('FUNCTION START: toc_checker')
        self.toc_checker(df)
        logging.info('FUNCTION END: toc_checker')
        logging.info('FUNCTION START: str_preprocessing')
        self.cleaned_sections = [self.text_cleanup(
            sec)[0] for sec in self.sec_list]
        logging.info('FUNCTION END: str_preprocessing')
        df['entity'] = None
        df['section'] = None
        df['score'] = None
        df['section_entity'] = None
        df['is_relevant'] = None
        df['sub_section_entity'] = None
        df['sub_section_entity_score'] = None
        df['is_sub_section_relevant'] = None

        self.get_lookup_table_df(df)
        self.detect_section_end(df)
        self.section_end_info(df)

        print(f"section extraction started:- {self.document_name}")
        df = self.section_logic(df)
        self.suppress_section(df)
        print(f"section extraction completed:- {self.document_name}")

        self.get_main_section_dict(df)
        # self.is_section_end_present(df)
        self.is_section_end_presentv1(df, self.sec_sub_intersec_dic, 'entity')
        # self.update_main_section(df)
        self.update_main_section_v2(df)
        self.update_main_section_v3(df)

        print(f"subsections extraction started:- {self.document_name}")
        self.update_section_to_sub_section(df)
        self.sub_section_logic(df)
        print(f"subsections extraction completed:- {self.document_name}")

        self.section_dates(df)
        df = self.get_lab_dates(df)

        print(f"chart order extraction started:- {self.document_name}")
        self.get_chart_order_0_v1(df)
        self.chart_order_logic(df)
        self.concat_files()
        print(f"chart order extraction completed:- {self.document_name}")

        df = self.subsection_mapping(df)
        
        df.rename(columns={'sub_section_entity': 'sub_section'}, inplace=True)
        file_name = self.textract_file.split("/")[-1]
        main_section_ls = df['updated_main_section'].value_counts().index
        df_met_ = []
        for i in main_section_ls:
            for j in list(set(df[(df['updated_main_section'] == i) & (df['section_entity'] == "SECTION")]['entity'].values)):
                page_ls = df[(df['updated_main_section'] == i) & (df['section_entity'] == 'SECTION') & (df['entity'] == j)]['Page'].to_list()
                df_met_.append([file_name,i,"SECTION",j,'',page_ls])
        for i in main_section_ls:
            for j in list(set(df[(df['updated_main_section'] == i) & (df['section_entity'] == "SUB SECTION")]['entity'].values)):
                for k in list(set(df[(df['updated_main_section'] == i) & (df['section_entity'] == "SUB SECTION") & (df['entity'] == j)]['sub_section'].value_counts().index)):
                    page_ls = df[(df['updated_main_section'] == i) & (df['section_entity'] == 'SUB SECTION') & (df['entity'] == j) & (df['sub_section'] == k)]['Page'].to_list()
                    df_met_.append([file_name,i,"SUB SECTION",j,k,page_ls])
        df_met_ = pd.DataFrame(df_met_)
        df_met_.rename(columns={0:'File_Name',1:'main_section',2:'section_entity',3:'entity',4:'sub_section_entity',5:'Pages'},inplace=True)
        df_temp = df.groupby(['Page', 'updated_line_number'])['Text'].apply(" ".join).reset_index()
        df_temp.rename(columns = {'Text':'combined_text'}, inplace = True)
        df = df.merge(df_temp, on=['Page', 'updated_line_number'], how='left')
        # logs_dict = {}
        # for i in list(set(df[df['section_entity'] == 'SECTION']['entity'].values)):
        #     logs_dict[i] = sorted(list(set(
        #         df[(df['section_entity'] == 'SECTION') & (df['entity'] == i)]['Page'].to_list())))

        # if "sub_section_entity" in df.columns:
        #     for i in list(set(df[df['section_entity'] == 'SUB SECTION']['sub_section_entity'].values)):
        #         logs_dict["Subsection "+i] = sorted(list(set(df[(df['section_entity'] == 'SUB SECTION') & (
        #             df['sub_section_entity'] == i)]['Page'].to_list())))
        try:
            csv_buf = StringIO()
            pd.concat([df_met_, df[df['section_entity'] == 'SECTION'][['Page', 'Text', 'entity', 'updated_main_section', 'score', 'updated_line_number', 'combined_text']]]).reset_index().to_csv(csv_buf, header=True, index=False, sep="|")
            self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(),
                                Key=rf"{save_path}/logs/section_subsection_logs_v1.txt")
        except:
            csv_buf = StringIO()
            df_met_.to_csv(csv_buf, header=True, index=False, sep="|")
            self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(),
                             Key=rf"{save_path}/logs/section_subsection_logs_v1.txt")
        df.drop(columns={'combined_text'}, inplace=True)
        
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
        ), Key=f'{save_path}/{self.file_name.replace(".csv","")}_section_subsection.csv')
        csv_buf.seek(0)

        # csv_buf = StringIO()
        # self.df_end.to_csv(csv_buf, header=True, index=False)
        # self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
        # ), Key=f'{save_path}/physician_info_{self.file_name.replace(".csv",".csv")}')
        # csv_buf.seek(0)

        # --------------- local ---------------
        # df.to_csv(rf'{save_path}\{self.file_name.replace(".csv","")}_section_subsection.csv',
        #           header=True, index=False)
        # --------------- end ---------------
        logging.info('FUNCTION END: saved the section subsection csv')
        return df
