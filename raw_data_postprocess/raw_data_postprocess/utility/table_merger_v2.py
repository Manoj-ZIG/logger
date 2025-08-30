
from rapidfuzz import fuzz, process
import pandas as pd
from io import StringIO
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor



class TableMerger:
    def __init__(self, s3_client, bucket_name, tables_df_dict, table_csv_file_ordered, master_comp_list, table_meta_data, zai_provider_emr_mapping_threshold_table_dict):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.tables_df_dict = tables_df_dict
        self.table_csv_file_ordered = table_csv_file_ordered
        self.master_comp_list = master_comp_list
        self.table_meta_data = table_meta_data
        self.zai_provider_emr_mapping_threshold_table_dict = zai_provider_emr_mapping_threshold_table_dict

    def read_csv_from_s3(self,csv_file_name):
        csv_data_obj = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=f"{self.table_csv_key}/{csv_file_name}")
        csv_data_body = csv_data_obj['Body'].read().decode('utf-8')
        csv_data = pd.read_csv(
            StringIO(csv_data_body))
        return csv_data
    
    def check_col_date(self, df):
        date = False
        col_name_ = [str(col) for col in df.columns]
        df_ = df.copy()
        df_.columns = [str(i) if x in ['nan','none','None','NaN'] else x for i, x in enumerate(col_name_, 1)]
        for col in df_.columns:
            crp = " ".join(df_[col].apply(lambda x: str(x)))
            detected_date_col = DatetimeExtractor.get_date_time_from_corpus_v2(crp, [
                'None'])

            if len(detected_date_col) >= 0.8*df.shape[1]:
                date = True
        return date
        

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
        # check if any first 2 rows has date
        for row_idx in range(0, 2):
            try:
                corpus = ' '.join(df.iloc[row_idx].apply(lambda x: str(x)))
                detected_date_row = DatetimeExtractor.get_date_time_from_corpus_v2(corpus, [
                    'None'])
                if len(detected_date_row) >= 0.5*df.shape[1] :
                    date = True
            except IndexError as e:
                pass
        # check if any column has dates
        # col_name_ = [str(col) for col in df.columns]
        # df_ = df.copy()
        # df_.columns = [str(i) if x in ['nan','none','None','NaN'] else x for i, x in enumerate(col_name_, 1)]
        # for col in df_.columns:
        #     crp = " ".join(df_[col].apply(lambda x: str(x)))
        #     detected_date_col = DatetimeExtractor.get_date_time_from_corpus_v2(crp, [
        #         'None'])

        #     if len(detected_date_col) >= 1 if (df_.shape[1]-1) <= 3 else 2:
        #         date = True
        return date
    
    def check_header_component(self, df):
        component_cnt = 0
        flag = False
        # check if any column has component value
        col_name_ = [str(col) for col in df.columns]
        df_ = df.copy()
        df_.columns = [str(i) if x in ['nan', 'none', 'None', 'NaN']
                       else x for i, x in enumerate(col_name_, 1)]
        
        for i in range(0, 2) :
            try:
                component_ls = list(df_.iloc[i, :])
                # component_ls = list(filter(lambda x: str(x).lower() != 'none', component_ls))
                # component_ls = list(filter(lambda x: str(x).lower() != 'nan', component_ls))
                for comp in component_ls:
                    if str(comp).lower().strip() not in ['nan', 'none']:
                        matcher = process.extractOne(
                            str(comp).lower(), self.master_comp_list, scorer=fuzz.token_sort_ratio)
                        if matcher[1] >= 60:
                            component_cnt += 1

                if component_cnt >= len(component_ls)*0.25:
                    flag = True
            except :
                pass

        return flag
        

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
        col_name_ = [str(col) for col in df.columns]
        df_ = df.copy()
        df_.columns = [str(i) if x in ['nan', 'none', 'None', 'NaN']
                       else x for i, x in enumerate(col_name_, 1)]
        df_.fillna("", inplace =True)
        for col in df_.columns:
            component_ls = df_[col].apply(lambda x: str(x))
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
        for page, df, bb_header, bb_table in df_ls:
            try:
                if int(page[0]) == int(page_df_tuple[0]) and df.equals(page_df_tuple[1]) \
                        or int(page[1]) == int(page_df_tuple[0]) and df.equals(page_df_tuple[1]):
                    flag = True
                    break
            except IndexError as e:
                pass
        return flag
    def strip_edge_null_cols( self, curr_df, next_df):
        def strip_first_null_col(df):
            
            first_column = df.iloc[:, 0]
            null_count = first_column.isnull().sum()
            if(null_count >= 0.9*df.shape[0] ):
                df = df.iloc[:, 1:]
                df.columns = [str(i) for i in  range(df.shape[1])]
            return df
        def strip_last_null_col(df):
            last_column = df.iloc[:, -1]
            null_count_last = last_column.isnull().sum()
            if(null_count_last == df.shape[0]):
                df = df.iloc[:, :-1]
            return df
        def drop_personal_info(df):
            exclude_terms = ['fac', 'facility', 'loc:', 'bed:','visit', 'location', 'hospital', "center", "birth", "num:", "family"]
            for row_index in range(2):
                row = list(df.iloc[row_index, :])
                try :
                        for terms in row:
                            if terms == terms:
                                terms_list = str(terms).split(" ")
                                for term in terms_list:
                                    matcher = process.extractOne(
                                        str(term).lower().strip(), exclude_terms, scorer=fuzz.token_sort_ratio)
                                    if matcher[1] >= 85:
                                        df = df.drop(index=df.index[row_index])
                                        break
                except Exception as e:
                    pass
            return df               
                            
        curr_df = drop_personal_info(curr_df)
        next_df = drop_personal_info(next_df)
                             
        curr_df = strip_first_null_col(curr_df)
        next_df =  strip_first_null_col(next_df) 
        len_diff = len(curr_df.columns) - len(next_df.columns)
        if len_diff == 1:
            curr_df = strip_last_null_col(curr_df)
        if len_diff == -1:
            next_df = strip_last_null_col(next_df)
        
    
        return curr_df, next_df

    def get_merger_meta_data(self, ):
        """   
        function use to merge the possible table.
        args:
            -
        returns:
            list: [(pageNo,pandas dataframe,bb_header), ...]
        """
        if '/' in self.table_csv_file_ordered[0]:
            tbl_files_ = [i.split("/")[-1] for i in self.table_csv_file_ordered]
        else:
            tbl_files_ = self.table_csv_file_ordered
        df_dict = {}
        merged_df_file = []
        flag = int(self.zai_provider_emr_mapping_threshold_table_dict['rule_value']['drop_irrelevant_columns'])
        next_table_start = float(self.zai_provider_emr_mapping_threshold_table_dict['rule_value']['next_table_start'])
        current_table_start = float(self.zai_provider_emr_mapping_threshold_table_dict['rule_value']['current_table_start'])
        next_table_threshold = eval(self.zai_provider_emr_mapping_threshold_table_dict['rule_value']['next_table_threshold'])
        min_tbl_threshold, max_tbl_threshold = next_table_threshold
        # iterate through each file
        for idx, tbl_f in enumerate(tbl_files_):
            curr_page_no = int(tbl_f.split("_")[0])
            # curr_df = self.read_csv_from_s3(tbl_f)
            curr_df = self.tables_df_dict.get(tbl_f)
            bb_header, bb_table = self.table_meta_data.get(tbl_f)

            
            for sub_idx, next_tbl_file in enumerate(tbl_files_[idx+1:]):
                next_page = int(next_tbl_file.split("_")[0])
                # next_df = self.read_csv_from_s3(next_tbl_file)
                next_df = self.tables_df_dict.get(next_tbl_file)
                # bb_top, bb_left, bb_height, bb_width
                nextdf_bb_header, nextdf_bb_table = self.table_meta_data.get(next_tbl_file)
                curr_df_end = 1 - (bb_table[0] + bb_table[2])
                next_df_start = nextdf_bb_table[0]
                next_tbl_threshold = int(next_tbl_file.split("_")[2])
                if(flag == 1) :
                    try:
                        curr_df, next_df = self.strip_edge_null_cols(curr_df, next_df)
                    except Exception as e:
                        pass
                if int(next_page) == int(curr_page_no)+1 and len(curr_df.columns) == len(next_df.columns)\
                         and curr_df_end <= current_table_start and next_df_start <= next_table_start and tbl_f not in merged_df_file \
                            and (min_tbl_threshold <= next_tbl_threshold and next_tbl_threshold <= max_tbl_threshold) :
                        if self.check_component(next_df) and self.check_component(curr_df)  and not self.check_date(next_df) :
                            next_df.columns = curr_df.columns
                            return_df = pd.concat([curr_df, next_df])
                            df_dict[tbl_f] = (tuple([curr_page_no, next_page]), return_df, bb_header, bb_table)
                            merged_df_file.append(next_tbl_file)
                        elif self.check_col_date(curr_df) and self.check_col_date(next_df) and self.check_header_component(curr_df):
                            next_df.columns = curr_df.columns
                            return_df = pd.concat([curr_df, next_df])
                            df_dict[tbl_f] = (tuple([curr_page_no, next_page]), return_df, bb_header, bb_table)
                            merged_df_file.append(next_tbl_file)
                        else :
                            return_df = curr_df
                            df_dict[tbl_f]  = (tuple([curr_page_no]), return_df, bb_header, bb_table)                            

                elif (int(next_page) >= int(curr_page_no)) and not self.check_existed_df(df_dict.values(), (curr_page_no, curr_df)) and tbl_f not in merged_df_file:
                    return_df = curr_df
                    df_dict[tbl_f] = (tuple([curr_page_no]), return_df, bb_header, bb_table)
                    if not (int(curr_page_no) == int(next_page) or int(curr_page_no)+1 == int(next_page)):
                        break
                else:
                    if not (int(curr_page_no) == int(next_page) or int(curr_page_no)+1 == int(next_page)):
                        break
                
            if not tbl_files_[idx+1:]:
                df_dict[tbl_f] = ((tuple([curr_page_no]), curr_df, bb_header, bb_table))
        return df_dict
