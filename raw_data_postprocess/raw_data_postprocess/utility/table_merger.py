
from rapidfuzz import fuzz, process
import pandas as pd
from io import StringIO
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor



class TableMerger:
    def __init__(self, s3_client, bucket_name, table_csv_key, table_csv_file_ordered, master_comp_list, table_meta_data):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.table_csv_key = table_csv_key
        self.table_csv_file_ordered = table_csv_file_ordered
        self.master_comp_list = master_comp_list
        self.table_meta_data = table_meta_data

    def read_csv_from_s3(self,csv_file_name):
        csv_data_obj = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=f"{self.table_csv_key}/{csv_file_name}")
        csv_data_body = csv_data_obj['Body'].read().decode('utf-8')
        csv_data = pd.read_csv(
            StringIO(csv_data_body))
        return csv_data

    def check_date(self, df):
        """  
        args:
            pandas dataframe: df
        return:
            bool:True/False
        Function use to check any given column/columns name has date or not
        """
        date = False
        # check if any columns name contain dates
        corpus = " ".join(pd.Series(df.columns).apply(lambda x: str(x)))
        detected_date_ = DatetimeExtractor.get_date_time_from_corpus_v2(corpus, [
            'None'])
        if len(detected_date_) >= df.shape[1]-1:
            date = True
        # check if any first 2 rows has date
        for row_idx in range(0, 2):
            try:
                corpus = ' '.join(df.iloc[row_idx].apply(lambda x: str(x)))
                detected_date_row = DatetimeExtractor.get_date_time_from_corpus_v2(corpus, [
                    'None'])
                if len(detected_date_row) >= df.shape[1]-1:
                    date = True
            except IndexError as e:
                pass
        # check if any column has dates
        for col in df.columns:
            crp = " ".join(df[col].apply(lambda x: str(x)))
            detected_date_col = DatetimeExtractor.get_date_time_from_corpus_v2(crp, [
                'None'])

            if len(detected_date_col) >= df.shape[0]-1:
                date = True
        return date

    def check_component(self, df):
        """  
        args:
            df
        return:
            bool
        function use to check if any columns has defined compoent values or not
        """
        component_cnt = 0
        flag = False
        # check if any column has component value
        for col in df.columns:
            component_ls = df[col].apply(lambda x: str(x))
            for comp in component_ls:
                matcher = process.extractOne(
                    str(comp).lower(), self.master_comp_list, scorer=fuzz.token_sort_ratio)
                if matcher[1] >= 60:
                    component_cnt += 1

            if component_cnt >= len(component_ls)*0.25:
                flag = True

        return flag

    def check_existed_df(self, df_ls, page_df_tuple):
        """  
        args:
            list: df_ls list of tuple((curr_page,next_page), df) 
            tuple: (current_page, df)
        return:
            bool
        """
        flag = False
        for page, df, bb_header in df_ls:
            try:
                if int(page[0]) == int(page_df_tuple[0]) and df.equals(page_df_tuple[1]) \
                        or int(page[1]) == int(page_df_tuple[0]) and df.equals(page_df_tuple[1]):
                    flag = True
                    break
            except IndexError as e:
                pass
        return flag

    def get_merger_meta_data(self, ):
        """   
        function use to merge the possible table.
        args:
            -
        returns:
            list: [(pandas dataframe, pageNo), ...]
        """
        if '/' in self.table_csv_file_ordered[0]:
            tbl_files_ = [i.split("/")[-1] for i in self.table_csv_file_ordered]
        else:
            tbl_files_ = self.table_csv_file_ordered
        df_ls = []
        merged_df_file = []
        # iterate through each file
        for idx, tbl_f in enumerate(tbl_files_):
            curr_page_no = int(tbl_f.split("_")[0])
            curr_df = self.read_csv_from_s3(tbl_f)
            bb_header = self.table_meta_data.get(tbl_f)

            next_df_score_ls = []
            for sub_idx, next_tbl_file in enumerate(tbl_files_[idx+1:]):
                next_page = int(next_tbl_file.split("_")[0])
                next_df = self.read_csv_from_s3(next_tbl_file)
          

                if int(next_page) == int(curr_page_no)+1 and len(curr_df.columns) == len(next_df.columns)\
                        and not self.check_date(next_df) and self.check_component(next_df) and self.check_component(curr_df) and tbl_f not in merged_df_file and max(next_df_score_ls if len(next_df_score_ls) > 0 else [True]) <= 70:
                    return_df = pd.concat([curr_df, next_df])
                    df_ls.append(
                        (tuple([curr_page_no, next_page]), return_df, bb_header))
                    merged_df_file.append(next_tbl_file)

                elif (int(next_page) >= int(curr_page_no)) and not self.check_existed_df(df_ls, (curr_page_no, curr_df)) and tbl_f not in merged_df_file:
                    return_df = curr_df
                    df_ls.append((tuple([curr_page_no]), return_df, bb_header))
                    if not (int(curr_page_no) == int(next_page) or int(curr_page_no)+1 == int(next_page)):
                        break
                else:
                    if not (int(curr_page_no) == int(next_page) or int(curr_page_no)+1 == int(next_page)):
                        break
                next_df_score_ls.append(int(next_tbl_file.split("_")[2]))
            if not tbl_files_[idx+1:]:
                df_ls.append((tuple([curr_page_no]), curr_df, bb_header))
        return df_ls
