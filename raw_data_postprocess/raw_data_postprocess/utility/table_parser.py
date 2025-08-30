from rapidfuzz import fuzz, process
import json
import pandas as pd
import numpy as np
import re
import itertools
import string
# import logging
# logging.basicConfig(level=logging.DEBUG)
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    import datetime_module.date_finder as dtf
    import datetime_module.time_finder as tf
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..datetime_module import date_finder as dtf
    from ..datetime_module import time_finder as tf
   

class TableParser:
    def __init__(self, lab_test_list, type_tag_dict, unit_reference_list):

        self.lab_test_list = lab_test_list
        self.type_tag_dict = type_tag_dict
        self.unit_reference_list = unit_reference_list
        

    def clean_str(self, str_):
        """ 
        clean_str function helps to remove special charecter from given string.
        """
        remove_char = """ '",.!():@#$%- """
        for sub_ltr in str(str_):
            if sub_ltr in remove_char:
                str_ = str_.replace(sub_ltr, '')
        return str(str_)

    def transpose_df(self, df):
        """
        this function help to transpose given dataframe
        args: 
            df (pandas dataframe)
        return: 
            df (transpose df)
        """
        letters = list(
            itertools.chain(
                string.ascii_uppercase,
                (''.join(pair)
                    for pair in itertools.product(string.ascii_uppercase, repeat=2))
            ))
        df_transpose = df.transpose()
        k_ = [letters[i] for i in df_transpose.columns]
        df_transpose.columns = k_
        df_transpose = df_transpose.reset_index()
        df_transpose.columns = list(range(len(df_transpose.columns)))
        df_transpose.columns = df_transpose.columns.astype(str).to_list()
        return df_transpose

    def column_checker(self, df):
        """ 
        This function use to detect whether column contain series of number or not
        args:
            df (pandas DataFrame)
        return:
            bool (True/False)
        """
        
        col_name = list(df.columns)
        col_name = [str(col) for col in col_name]
        comp_ls = [str(i) for i in range(len(col_name))]
        comp_ls_for_non_numeric = [str(i+1) for i in range(len(col_name[2:]))]
        # if col_name == comp_ls:
        if len(set(col_name).symmetric_difference(set(comp_ls))) == 0:
            return True
        # elif comp_ls_for_non_numeric == [self.clean_str(i) for i in col_name[2:]]:
        elif len(set(comp_ls_for_non_numeric).symmetric_difference(set([self.clean_str(i) for i in col_name[2:]]))) == 0:
            return True
        else:
            return False

    def zeroth_column_checker(self, df):
        """ 
        Function which help to check if zeroth column of dataFrame has null values
        args:
            df
        return:
            bool
        """
        if df[df.columns[0]].isnull().all():
            return True
        else:
            return False

    def check_lab_str(self, lab_test, thresh=67):
        """ 
        given any column elements/values function use to detect whether column has defined lab_test values or not
        args:
            list (lab_test)
        return:
            bool (True/False)
        """
        cnt_label = 0
        lab_ls = self.lab_test_list
        # additional_variant_lab_ls = self.variant_or_abbreviation_lab_list

        for lab_test_ in list(set(lab_test)):
            if str(lab_test_).lower().strip() not in ['nan', 'none']:
                matcher_lab_str = process.extractOne(
                    str(lab_test_).lower(), lab_ls, scorer=fuzz.token_sort_ratio)
                # check if lab_ls str present in column
                if matcher_lab_str[1] >= thresh:
                    cnt_label += 1
        if len(lab_test) >= 10:
            return True if cnt_label >= min(len(self.type_tag_dict), len(lab_test)*.08) else False
        else:
            return True if cnt_label >= min(len(self.type_tag_dict), len(lab_test)*.30) else False

    def lab_matcher(self, lab_test, thresh=67):
        """ 
        given any column elements/values function use to check the lab test name
        args:
            list (lab_test)
        return:
            bool (True/False)
        """
        cnt_label = 0
        lab_ls = self.lab_test_list
        # additional_variant_lab_ls = self.variant_or_abbreviation_lab_list
        lab_test = list(filter(lambda x: x.lower() != 'nan', lab_test))
        lab_test = list(filter(lambda x: x.lower() != 'none', lab_test))

        for lab_test_ in lab_test:
            if str(lab_test_).lower().strip() not in ['nan', 'none']:
                matcher_lab_str = process.extractOne(
                    str(lab_test_).lower(), lab_ls, scorer=fuzz.token_sort_ratio)
                # check if lab_ls str present in column
                if matcher_lab_str[1] >= thresh:
                    cnt_label += 1
        return True if cnt_label > 0 else False

    def lab_matcher_count(self, lab_test, thresh=67):
        """ 
        function helps to return count of lab_matcher terms.
        By matching each element of the input list with lab_list (lab master for a particular lab)
        args:
            list (lab_test)
        return:
            int (count)
        """
        cnt_label = 0
        lab_ls = self.lab_test_list
        # additional_variant_lab_ls = self.variant_or_abbreviation_lab_list
        lab_test = list(filter(lambda x: x.lower() != 'nan', lab_test))
        lab_test = list(filter(lambda x: x.lower() != 'none', lab_test))
        
        for lab_test_ in lab_test:
            if str(lab_test_).lower().strip() not in ['nan', 'none']:
                matcher_lab_str = process.extractOne(
                    str(lab_test_).lower(), lab_ls, scorer=fuzz.token_sort_ratio)
                # check if lab_ls str present in column
                if matcher_lab_str[1] >= thresh:
                    cnt_label += 1
        return cnt_label

    def check_lab_val_str(self, lab_test_val, lab_ls_val=[''], if_int=True, thresh=54):
        """ 
        function help to detect whether lab_values are not present in proper column,
        used to forced mapped column
        args:
            list
        return:
            bool 
        """
        cnt_label_val = 0
        lab_test_val = list(filter(lambda x: x != 'nan', lab_test_val))
        lab_test_val = list(filter(lambda x: x != 'none', lab_test_val))

        for lab_test_ in lab_test_val:
            if str(lab_test_).lower().strip() not in ['nan', 'none']:
                matcher_lab_val_str = process.extractOne(
                    str(lab_test_).lower(), lab_ls_val, scorer=fuzz.token_sort_ratio)

                # check if same column contain values of lab test (force mapped to value col)
                if matcher_lab_val_str[1] >= thresh:
                    cnt_label_val += 1
        return True if cnt_label_val > 0 else False

    def remove_null_col(self, df):
        """ 
        function use to remove null column if its present in given dataFrame.
        """
        ls = []
        for col in df.columns:
            if df[col].isna().all():
                continue
            else:
                ls.append(col)
        return ls

    def lab_val_col_checker(self, df):
        """ 
        function use to anchor value/test_result column.
        """
        lab_val_ls = ['value', 'val']
        exclusion_ls = ['ref range', 'range']

        # condition1
        lab_val_co_name = None
        for lab_val_ in df.columns:
            if str(lab_val_).lower() in lab_val_ls and str(lab_val_).lower() not in exclusion_ls:
                lab_val_co_name = lab_val_
        return lab_val_co_name

    def date_checker(self, st):
        """ 
        function which help to check if any column has date
        args: 
            list or dataFrame column
        return:
            bool

        """
        valid_date_condition = None
        date_cnt_condition = None
        if isinstance(st, list):
            st = [str(i) for i in st]
        else:
            st = st.apply(lambda x: str(x))
            st = list(st)
        if isinstance(st, list):
            dtcol = dtf.run(' | '.join(st))
            timecol = tf.run(' | '.join(st))
            date_ls = pd.json_normalize(json.loads(dtcol))
            time_ls = DatetimeExtractor.validate_time(' '.join(st))
            filter_time_ls = DatetimeExtractor.suppress_datetime(time_ls)
            suppressed_date_ls = list(filter(lambda x: x != 'nan', st))
            if date_ls.shape[0] > 0:
                date_ls = date_ls[date_ls['text'].apply(
                    lambda x: len(str(x)) > 4)]
                date_ls_yr = all(date_ls[(date_ls['year'] >= 2000) &
                                         (date_ls['year'] <= 2025)])
                date_ls_month = all(
                    date_ls['month'].apply(lambda x: 1 <= x <= 12 if isinstance(x, (int)) else 0))
                date_ls_day = all(date_ls['day'].apply(
                    lambda x: 1 <= x <= 31 if isinstance(x, (int)) else 0))
                date_ls_text = all(date_ls['text'].apply(
                    lambda x: len(str(x)) > 4 if isinstance(x, (int)) else str(0)))
                valid_date_condition = any(
                    [date_ls_day, date_ls_month, date_ls_text])
                date_cnt_condition = True if len(
                    date_ls) >= len(suppressed_date_ls)-1 else False
                if filter_time_ls:
                    return True if (all((date_cnt_condition, valid_date_condition)) or len(filter_time_ls) >= len(st)/2) else False
                else:
                    return True if all((date_cnt_condition, valid_date_condition)) else False
            elif filter_time_ls:
                return True if len(filter_time_ls) >= len(st)/2 else False
            else:
                return False
    def date_checker_pattern(self, st):
        """ 
        function which help to check if any column has date
        args: 
            list or dataFrame column
        return:
            bool

        """
        valid_date_condition = None
        date_cnt_condition = None
        if isinstance(st, list):
            st = [str(i) for i in st]
        else:
            st = st.apply(lambda x: str(x))
            st = list(st)
        if isinstance(st, list):
            dtcol = dtf.run(' | '.join(st))
            timecol = tf.run(' | '.join(st))
            date_ls = pd.json_normalize(json.loads(dtcol))
            time_ls = DatetimeExtractor.validate_time(' '.join(st))
            filter_time_ls = DatetimeExtractor.suppress_datetime(time_ls)
            suppressed_date_ls = list(filter(lambda x: x != 'nan', st))
            if date_ls.shape[0] > 0:
                date_ls = date_ls[date_ls['text'].apply(
                    lambda x: len(str(x)) > 4)]
                date_ls_yr = all(date_ls[(date_ls['year'] >= 2000) &
                                         (date_ls['year'] <= 2025)])
                date_ls_month = all(
                    date_ls['month'].apply(lambda x: 1 <= x <= 12 if isinstance(x, (int)) else 0))
                date_ls_day = all(date_ls['day'].apply(
                    lambda x: 1 <= x <= 31 if isinstance(x, (int)) else 0))
                date_ls_text = all(date_ls['text'].apply(
                    lambda x: len(str(x)) > 4 if isinstance(x, (int)) else str(0)))
                valid_date_condition = any(
                    [date_ls_day, date_ls_month, date_ls_text])
                date_cnt_condition = True if len(
                    date_ls) >= len(suppressed_date_ls)-1 else False
                
                return True if all((date_cnt_condition, valid_date_condition)) else False
    
            else:
                return False

    def check_whole_df_isna(self, df):
        """ 
        check the given dataframe has all null column
        """
        if df.isnull().all().all():
            return True
        else:
            return False

    def check_for_ref_col(self, txt_ls):
        """ 
        function use to check if any column has range value(12-45 bpm) present,
        in order to supress the column for value Extraction
        args:
            list
        return:
            bool
        """
        ref_cnt = 0
        for txt in txt_ls:
            m = re.match(r"\b(\d\d?\.?\d?\d\s*-?\s*\d\d?\.?\d?\d?)\b",
                         str(txt).strip(), re.I)
            special_case_ref_col = re.match(
                r"(\[.*-?.*\])", str(txt).strip(), re.I)
            if (m and len(txt) > 3) or (special_case_ref_col):
                ref_cnt += 1
        return True if ref_cnt >= len(txt_ls)/2 else False

    def check_for_unit_col(self, ls, col_name):
        """ 
        function use to anchor the unit column in detected table.
        """
        cnt = 0
        unit_col_list = ['units', 'unit']
        if col_name.strip().lower() in unit_col_list:
            return True
        else:
            for element in ls:
                match_ratio = process.extractOne(str(element), self.unit_reference_list,
                                                 scorer=fuzz.token_sort_ratio)
                if match_ratio[1] > 85:
                    cnt += 1
            return True if cnt >= 1 else False

    def check_for_str_to_int_ratio(self, df):
        """ function use to check string/int ratio, in order to suprress the unusefull column being get parsed
        args:
            df: (pandas dataframe)
        return:
            str (column name, which has min ratio)
        """
        ratio_dict = {}
        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x))
            corpus = ' '.join(df[col].to_list())
            num_ = re.findall(r"\d+", corpus, re.I)
            str_ = re.findall(r"[a-z]", corpus, re.I)
            ratio_dict[col] = len(str_)/(len(num_)+1)
        return min(ratio_dict)
    
    def merge_date_time(self, df):
        col_date, col_time = False, False
        def count_dt(date_lst):
            if date_lst:
                df = pd.DataFrame(date_lst)
                count = df[df[1] == True][1].sum()
                return count
        for idx, col_name in enumerate(list(df.columns)):
            datetime_col = DatetimeExtractor.get_date_time_from_corpus_v2(' '.join(df.iloc[:, idx].map(lambda x: str(x))), ['None'])
            date_time_df = pd.DataFrame(datetime_col)
            if date_time_df.shape[0] > 0 and date_time_df[0].count() > 0.6*df.shape[0] and date_time_df[1].count() > 0.6*df.shape[0]:
                continue
            else:
                date_col = DatetimeExtractor.validate_date(' '.join(df.iloc[:, idx].apply(lambda x: str(x))))
                if count_dt(date_col) is not None and count_dt(date_col) > 0.6*df.shape[0]:
                    col_date = str(col_name)
                time_col = DatetimeExtractor.validate_time(' '.join(df.iloc[:, idx].apply(lambda x: str(x))))
                if count_dt(time_col) is not None and  count_dt(time_col) > 0.6*df.shape[0]:
                    col_time = str(col_name)
        if col_date and col_time and (col_date != col_time):
            df.columns = df.columns.astype(str)
            # df['DateTime'] = df[col_date].str.cat(df[col_time].astype(str), sep=" ")
            df['DateTime'] = df.apply(lambda row: str(row[col_date]) + ' ' + str(row[col_time]), axis=1)
            df = df.drop([col_date, col_time], axis = 1)
        return df
    
    def merge_row_date_time(self, df):
        flag  = False
        row_date, row_time = False, False
        def count_dt(date_lst):
            if date_lst:
                df = pd.DataFrame(date_lst)
                count = df[df[1] == True][1].sum()
                return count
        df = df.reset_index(drop = True)
        for idx, row_name in enumerate(df.index):
            datetime_row = DatetimeExtractor.get_date_time_from_corpus_v2(" ".join(df.loc[row_name].map(lambda x: str(x)).values), ['None'])
            date_time_df = pd.DataFrame(datetime_row)
            if date_time_df.shape[1] > 0 and date_time_df[0].count() >= 0.5*df.shape[1] and date_time_df[1].count() > 0.6*df.shape[1]:
                continue
            else:
                date_row = DatetimeExtractor.validate_date(" ".join(df.loc[row_name].map(lambda x: str(x)).values))
                if count_dt(date_row) is not None and count_dt(date_row) >= 0.5*df.shape[1]:
                    row_date = str(row_name)
                time_row = DatetimeExtractor.validate_time(" ".join(df.loc[row_name].map(lambda x: str(x)).values))
                if count_dt(time_row) is not None and  count_dt(time_row) >= 0.5*df.shape[1]:
                    row_time = str(row_name)
            
            
            if row_date and row_time and (row_date != row_time):
                row_date = str(row_date)
                row_time = str(row_time)
                
                df.index = df.index.astype(str)
                df.loc[row_date, :] = df.loc[row_date,:].apply(str).values
                df.loc[row_time, :] = df.loc[row_time,:].apply(str).values
                df.loc[row_date] = df.loc[row_date].str.cat(df.loc[row_time], sep=" ", na_rep = '')
                df = df.drop([row_time], axis = 0)
                df.reset_index(drop = True, inplace = True)
                flag = True
                

                break
        return df, flag

    def get_multiheader_table_df(self, df):

        def filter_date_ls(ls):
            filter_date_list = [i for i in ls if len(re.findall(
                r'([^0-9])', i[0], re.I)) > 0 and len(i[0]) >= 4]
            return filter_date_list

        sub_df_list = []

        is_first_df, start_idx = True, 0
        for i, row in df.iterrows():

            detected_date_ls = DatetimeExtractor.get_date_time_from_corpus_v2(
                " ".join(row.apply(lambda x: str(x))), ['None'])
            detected_words_with_date = re.findall(
                r'([a-z])', " ".join(row[1:].apply(lambda x: str(x))), re.I)
            false_flag = True if len(detected_words_with_date) >= 3 else False
            flag = len(filter_date_ls(detected_date_ls)) >= int(
                len(row)*0.85) and not false_flag
            # print(i, flag, " ".join(row.apply(lambda x: str(x))))
            if flag == True:
                sub_df = df[start_idx: i].copy()
                sub_df.columns = df.iloc[start_idx].to_list(
                ) if not is_first_df else df.columns
                start_idx, is_first_df = i, False
                sub_df_list.append(sub_df)
            elif i + 1 == df.shape[0]:
                sub_df = df[start_idx:].copy()
                sub_df.columns = df.iloc[start_idx].to_list(
                ) if not is_first_df else df.columns
                start_idx, is_first_df = i, False
                sub_df_list.append(sub_df)
        return sub_df_list
    
    def preprocess_df(self, df):
        """ 
        function use to preprocess the given DataFrame
        args:
            df: (pandas dataframe)
        return
             df: (processed pandas dataframe)
        """
        # remove first col if it contain [0, 1, 2] and assign below row as column
        df_ = df.dropna(how='all', axis=1)
        if(df_.shape[1]>0 and df.shape[1] != df_.shape[1]):
            try :
                columns = [int(i) for i in df.columns]
                columns_ = [str(i) for i in range(df_.shape[1])]
                df_.columns = columns_
                df = df_
            except Exception as e:
                pass
            

        df, flag = self.merge_row_date_time(df)
        df.index = pd.RangeIndex(df.shape[0]) 

        # if self.column_checker(df) and not (self.check_lab_str(first_row_str) and self.date_checker(first_row_str) > 0):
        first_row = df.iloc[0].apply(lambda x: str(x)).to_list()
        first_row_detected_date = DatetimeExtractor.get_date_time_from_corpus_v2(
            " ".join(first_row), ['none'])
        for i, row in df.iterrows():
            row_list = row.apply(lambda x: str(x)).to_list()
            detected_date_ls = DatetimeExtractor.get_date_time_from_corpus_v2(
                " ".join(row_list), ['none'])
            date_flag = True if len(detected_date_ls) > 1 else False
            if self.column_checker(df) and (self.lab_matcher_count(row_list) >= 2 if len(row_list) <= 3 else self.lab_matcher_count(row_list) >= len(row_list)*0.26) \
                    and not (set(self.unit_reference_list).intersection([i.strip().lower() for i in row_list])):  # (-2 for date_col and comp_value column)
                df.columns = df.iloc[i]
                df_ = df.drop(i, axis=0)
                break
            elif self.column_checker(df) and (self.date_checker(row_list) and not self.lab_matcher(row_list)
                                              and (date_flag if len(row_list) >= 5 else True)):
                df.columns = df.iloc[i]
                df_ = df.drop(i, axis=0)
                break
            elif sum([int(self.date_checker([j])) for j in row_list])+1 == len(row_list):
                df.columns = df.iloc[i]
                df_ = df.drop(i, axis=0)
                break
            elif self.column_checker(df) and i == df.shape[0]-1 and not\
                    (len(first_row_detected_date) >= 1 and self.lab_matcher(first_row)):
                df.columns = df.iloc[0]
                df_ = df.drop(0, axis=0)
            else:
                df_ = df
        # check if column contain null value
        if df_.columns.isna().sum() > 0:
            col_ls = []
            for idx, col in enumerate(df_.columns):
                if col is np.nan:
                    col_ls.append(str(idx))
                else:
                    col_ls.append(str(col))
            df_.columns = col_ls

    
        if (not flag):
            # merge the seperate date and time columns as single column and delete individuals    
            df_ = self.merge_date_time(df_)
            is_date_present = False
            for col_name in range(len(df_.columns)):
                if self.date_checker(df_.iloc[:, col_name]) and not self.date_checker(df_.columns.to_list()) :
                    # move the column to the last position
                    df_ = df_.iloc[:, [col_name] + 
                            [col for col in range(len(df_.columns)) if col != col_name]]
                    is_date_present = True
                    break
            col_list = list(df_.columns)
            
            # restructure the dataframe if its find any date column
            for col_name in range(len(col_list)):
                if self.check_lab_str(df_.iloc[:, col_name]) and is_date_present == False:
                    # move the column to the last position
                    df_ = df_.iloc[:, [col_name] + 
                            [col for col in range(len(df_.columns)) if col != col_name]]
                    break
                elif self.check_lab_str(df_.iloc[:, col_name]) and is_date_present == True:
                    # move the column to the last position
                    df_.insert(1, 'lab_name', df_.iloc[:, col_name])
                    df_.drop(columns = [col_list[col_name]], inplace = True)
                    break
        # restructure the dataframe if its find any date column
        # for col_name in df_.columns:
        #     if self.date_checker(df_[col_name]) or self.check_lab_str(df_[col_name]):
        #         # move the column to the last position
        #         df_ = df_[[col_name] +
        #                   [col for col in df_.columns if col != col_name]]
        #         break
            # else:
            #     df_= df_

        # drop null columns
        

        # check if column contain null value
        if df_.columns.isna().sum() > 0:
            col_ls = []
            for idx, col in enumerate(df_.columns):
                if col is np.nan:
                    col_ls.append(str(idx))
                else:
                    col_ls.append(str(col))
            df_.columns = col_ls
        # # check column containing null data
        # for col in df_.columns:
        #     if df_[col].isna().sum() >= df_.shape[1]-1:
        #         df_ = df_.drop([col], axis=1)

        # check if first row contain standard col name
        s1 = set(['value', 'val', 'result', 'results',
                  'ref range', 'range', 'component', 'flag', 'date time', 'procedure', 'unit'])
        df_ = df_.reset_index(drop=True)
        for i, row in df_.iterrows():
            row_val = [str(i).lower().strip() for i in row.values.tolist()]
            row_val_ = [process.extractOne(
                comp_name, s1, scorer=fuzz.token_sort_ratio)[0] for comp_name in row_val if process.extractOne(
                comp_name, s1, scorer=fuzz.token_sort_ratio)[1] > 72]
            if len(s1.intersection(set(row_val_))) >= 2 and not self.date_checker(row_list) and not self.lab_matcher_count(row_list) >= len(row_list)*0.5:
                df_.columns = df_.iloc[i]
                df_ = df_.drop([i], axis=0)
                df_ = df_.reset_index(drop=True)
                break
        # check if column contain null value
        if df_.columns.isna().sum() > 0:
            col_ls = []
            for idx, col in enumerate(df_.columns):
                if col is np.nan:
                    col_ls.append(str(idx))
                else:
                    col_ls.append(str(col))
            df_.columns = col_ls
        return df_

    def pattern_1_special_case_checker(self, df):
        """  
        check the different type of pattern
        pattern_1: columns contains dates and first col contain lab_test
        args: 
           df: (pandas dataframe)
        return
            bool 
        """

        str_ls = df[df.columns[0]].apply(lambda x: str(x)).to_list()

        date_col = DatetimeExtractor.get_date_time_from_corpus_v2(
            ' '.join(df.columns.map(lambda x: str(x))), ['None'])
        time_col = DatetimeExtractor.suppress_datetime(
            DatetimeExtractor.validate_date(' '.join(df.columns.map(lambda x: str(x)))))

        date_tm_first_col = DatetimeExtractor.get_date_time_from_corpus_v2(
            ' '.join(str_ls), ['None'])
        time_first_col = DatetimeExtractor.suppress_datetime(
            DatetimeExtractor.validate_time(' '.join(str_ls)))

        if len(date_col) or len(time_col):
            if len(date_col) >= 1 and self.check_lab_str(str_ls):
                return True
            else:
                return False
        elif len(date_tm_first_col) or len(time_first_col):
            if (len(date_tm_first_col) >= (df.shape[0]*0.6) or len(time_first_col) >= (df.shape[0]*0.6)) \
                    and self.check_lab_str(df.columns.to_list()):
                return True
            else:
                return False
        else:
            return False
    def get_ref_units_col(self, df):
                
        reference_terms = ["reference", "reference range", "reference low high", "ref range", "range", "reference low - high"]
        unit_terms = ['units', "unit", "reference units", "range/units"]
        exclude_terms_index = {}
        
        for i, col_name in enumerate( df.columns):
            match_ratio_ref = process.extractOne(str(col_name).lower().strip(), reference_terms,
                                                 scorer=fuzz.token_sort_ratio)
            if match_ratio_ref[1] > 85:
                exclude_terms_index["reference"] = i
                break
        for i, col_name in enumerate( df.columns):
            match_ratio_units = process.extractOne(str(col_name).lower().strip(), unit_terms,
                                                 scorer=fuzz.token_sort_ratio)
            if match_ratio_units[1] > 85:
                exclude_terms_index['units'] = i
                break 
        return exclude_terms_index 

    def pattern_1_checker(self, df):
        """  
        check the different type of pattern
        pattern_1: columns contains dates and first col contain lab_test
        args: 
           df: (pandas dataframe)
        return
            bool 
        """
        str_ls = df[df.columns[0]].apply(lambda x: str(x)).to_list()

        date_col = DatetimeExtractor.get_date_time_from_corpus_v2(
            ' '.join(df.columns.map(lambda x: str(x))), ['None'])
        time_col = DatetimeExtractor.suppress_datetime(
            DatetimeExtractor.validate_time(' '.join(df.columns.map(lambda x: str(x)))))

        date_tm_first_col = DatetimeExtractor.get_date_time_from_corpus_v2(
            ' '.join(str_ls), ['None'])
        time_first_col = DatetimeExtractor.suppress_datetime(
            DatetimeExtractor.validate_time(' '.join(str_ls)))
        if len(date_col) or len(time_col):
            exclude_terms_index = self.get_ref_units_col(df)
            if(len(exclude_terms_index) > 0):
                if len(date_col) >= ((df.shape[1]- len(exclude_terms_index))*0.4) and self.check_lab_str(str_ls):
                    df_dropped = df.iloc[:, [i for i in range(df.shape[1]) if i not in list(exclude_terms_index.values())]]
                    return True, df_dropped
                else:
                    return False, df
            else :
                if len(date_col) >= (df.shape[1]*0.4) and self.check_lab_str(str_ls):
                    return True, df
                else:
                    return False, df
        elif len(date_tm_first_col) or len(time_first_col):
            if (len(date_tm_first_col) >= (df.shape[0]*0.6) or len(time_first_col) >= (df.shape[0]*0.6)) \
                    and self.check_lab_str(df.columns.to_list()):
                return True, df
            else:
                return False, df
        else:
            return False, df

    def pattern_2_checker(self, df):
        """  
        check the different type of pattern
        pattern_2: one of the column has date, likewise lab_test and it's value
        args: 
           df: (pandas dataframe)
        return
            bool 
        """
        return_dict = {}

        # check for each column and get the date/time | lab_test_name column name
        for idx, col in enumerate(df.columns):
            try:
                if self.check_lab_str(df[col].to_list()) and (('text' not in return_dict or 'lab_test_col' not in return_dict) and
                                                              not self.check_for_unit_col(df[col], col)):
                    if self.check_lab_val_str(df[col].to_list()):
                        return_dict['text'] = col
                    elif 'lab_test_col' not in return_dict:
                        return_dict['lab_test_col'] = col
                    # else:
                    #     return_dict['lab_test_col'] = col
                elif self.check_lab_val_str(df[col].to_list()):
                    return_dict['text'] = col

                elif self.date_checker_pattern(df[col]):
                    return_dict['date_col'] = col

                elif self.check_for_unit_col(df[col], col) and col not in return_dict.values():
                    return_dict['unit_col'] = col
            except Exception as e:
                pass

        rem_col = list(set(list(df.columns)).difference(
            list(return_dict.values())))
        # drop the column
        for col in rem_col:
            if df[col].isna().sum() >= df.shape[0] and df[col].isna().sum() != 0:
                df.drop([col], axis=1, inplace=True)

        rem_col = list(set(list(df.columns)).difference(
            list(return_dict.values())))
        # for text col
        lab_val_ls = ['value', 'val', 'result', 'results']
        exclusion_ls = ['ref range', 'range',
                        'reference range', 'flag', "range/units", "range/unit", "reference low high","reference low - high", "units", "unit", "reference","reference units"]
        # condition1
        lab_val_co_name = None
        for lab_val_ in df[rem_col].columns:
            if str(lab_val_).lower().strip() in lab_val_ls and str(lab_val_).lower().strip() not in exclusion_ls:
                lab_val_co_name = lab_val_
                return_dict['text'] = lab_val_
        # condition2 (ref and other columns)
        other_col_to_exclude = []
        if not lab_val_co_name:
            if df.shape[1] == len(return_dict.values())+1 and 'text' not in return_dict.keys():
                return_dict['text'] = rem_col[0]
            else:
                for rem_col_ in df[rem_col].columns:
                    if self.check_for_ref_col(df[rem_col_].to_list()) or str(rem_col_).lower().strip() in exclusion_ls:
                        other_col_to_exclude.append(rem_col_)

                rem_col_ = list(set(rem_col).difference(other_col_to_exclude))
                if rem_col_ and len(return_dict.keys()) + \
                        len(other_col_to_exclude)+1 == df.shape[1]:
                    return_dict['text'] = rem_col_[0]

        return return_dict

    def pattern_2a_checker(self, df):
        """  
        check the different type of pattern
        pattern_2a: one of the row has date, likewise lab_test and it's value
        args: 
           df: (pandas dataframe)
        return
            bool 
        """
        # check the column values to check [date/lab_test_name]
        return_dict = {}
        if self.check_lab_str(df.columns.to_list()):
            return_dict['lab_test_col'] = 'columns'
        elif self.date_checker_pattern(df.columns.to_list()):
            return_dict['date_col'] = 'columns'

        # check the rows if they contain [date/lab_test_name]
        for idx, row in df.iterrows():
            if self.check_lab_str(row.values.tolist()) and 'lab_test_col' not in return_dict.keys():
                return_dict['lab_test_col'] = idx
            elif self.date_checker_pattern(row.values.tolist()) and 'date_col' not in return_dict.keys():
                return_dict['date_col'] = idx
                
        if len(return_dict.keys()) >= 2:
            return True
        else:
            return False

    def check_lab_test_name_(self, str_):
        """  
        function use to tag the detected lab_test_name to predefined standard name
        args:
            str
        return:
            str
        """
        str_ = str(str_).lower()

        def get_the_tag_type(str_):
            for k, v in self.type_tag_dict.items():
                if str_.strip() in v:
                    return k
                  
        if str_ not in ['nan', 'none']:
            matcher = process.extractOne(
                str_, self.lab_test_list, scorer=fuzz.token_sort_ratio)
            margin_ratio = matcher[1]
            if margin_ratio >= 70:
                return matcher[0]
            elif get_the_tag_type(str_):
                return get_the_tag_type(str_)
            else:
                return 'Other'
        else:
            return 'Other'

    def get_extracted_value_for_pattern_1(self, df):
        """ 
        based on pattern_1, this function extract the values from dataframe, and return the extracted dict
        args:
            df: (pandas df)
        return:
            dict: ({'text':_, 'type':_, 'name':_,'date'_})
        """

        return_dict_ls = []
        # date as columns and lab_test in 1st column
        # if date_checker(df.columns[1:].to_list()):
        dtcol = dtf.run(' | '.join(df.columns[1:].map(lambda x: str(x))))
        date_ = pd.json_normalize(json.loads(dtcol))
        if date_.shape[1] > 0:
            for i, col in enumerate(df.columns[1:]):
                for name, val in zip(df[df.columns[0]], df.iloc[:, i+1]):
                    # clean the val
                    # val = re.sub("[^0-9]", "", val)
                    return_dict_ls.append(
                        {'date': col, 'type': self.check_lab_test_name_(name), 'text': val,  'name': name})

        # lab_test as columns and dates in 1st column
        else:
            for i, col in enumerate(df.columns[1:]):
                for name, val in zip(df[df.columns[0]], df.iloc[:, i+1]):
                    # val = re.sub("[^0-9]", "", val)
                    return_dict_ls.append(
                        {'date': name, 'type': self.check_lab_test_name_(col), 'text': val,  'name': col})

        return return_dict_ls

    def get_extracted_value_for_pattern_2(self, df):
        """  
        based on pattern_2, this function extract the values from dataframe, and return the extracted dict
        args:
            df: (pandas df)
        return:
            dict: ({'text':_, 'type':_, 'name':_,'date'_, 'unit':_})
        """
        return_dict_ls = []
        lab_date_col_name = self.pattern_2_checker(df)
        if lab_date_col_name.get('date_col'):
            df[lab_date_col_name.get(
                'date_col')] = df[lab_date_col_name.get('date_col')].ffill()
        for idx, row in df.iterrows():
            return_dict_ls.append({'date': row[lab_date_col_name.get('date_col')] if lab_date_col_name.get('date_col') else '',
                                   'type': self.check_lab_test_name_(row[lab_date_col_name.get('lab_test_col')]),
                                   'name': row[lab_date_col_name.get('lab_test_col')],
                                   'text': row[lab_date_col_name.get('text')],
                                   'unit': row[lab_date_col_name.get('unit_col')] if lab_date_col_name.get('unit_col') else '',
                                   })
        return return_dict_ls

    def get_result_from_table_csv(self, table_df):
        """  
        get the final result of extracted value from given csv_file (textract output csv)
        args:
            df: (merger df)
        return:
            dict: ('type'_, 'name':_,'text':_,'date':_,'unit':_)
        """

        preprocess_table_df = self.preprocess_df(table_df)
        try:
            if self.check_whole_df_isna(preprocess_table_df):
                return ( [],"null")

            # elif self.pattern_1_checker(preprocess_table_df):
            #     return self.get_extracted_value_for_pattern_1(preprocess_table_df)
            flag, df = self.pattern_1_checker(preprocess_table_df.copy())
            if flag:
                concat_result_of_subdf = []
                sub_df_list = self.get_multiheader_table_df(df.copy())
                for sub_df in sub_df_list:
                    concat_result_of_subdf += self.get_extracted_value_for_pattern_1(
                        sub_df)
                return ( concat_result_of_subdf, 'pattern1' if len(sub_df_list) == 1 else 'pattern1_multiheader')
            
            elif self.pattern_2a_checker(preprocess_table_df):
                return ( self.get_extracted_value_for_pattern_2(self.transpose_df(preprocess_table_df)), 'pattern_2a')

            elif self.pattern_2_checker(preprocess_table_df):
                return ( self.get_extracted_value_for_pattern_2(preprocess_table_df), 'pattern2')
            elif self.pattern_1_special_case_checker(preprocess_table_df):
                return ( self.get_extracted_value_for_pattern_1(preprocess_table_df), 'pattern1_special_case')
            else:
                return ( [], 'None')

        except Exception as e:
            return ([], 'Exception')
