import pandas as pd
from io import StringIO
try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from utility.date_suppression import DateSuppression
    from utility.date_suppression_labs import DateSuppressionLabs
    from constant.date_suppression_constant import section_list, suppress_date_tag, suppress_date_without_adm_disch, date_tags
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..utility.date_suppression import DateSuppression
    from ..utility.date_suppression_labs import DateSuppressionLabs
    from ..constant.date_suppression_constant import section_list, suppress_date_tag, suppress_date_without_adm_disch, date_tags


class GetResult:
    def __init__(self,s3_client,bucket_name, merged_tables_data, table_parser):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.merged_tables_data = merged_tables_data
        self.table_parser = table_parser

    def check_extracted_result_date(self, ext_val_list):
        detected_dates = [dict_.get('date')
                          for dict_ in ext_val_list if dict_.get('date')]
        return True if len(detected_dates) > 0 else False

    def read_csv_from_s3(self, csv_file_key):
        csv_data_obj = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=csv_file_key)
        csv_data_body = csv_data_obj['Body'].read().decode('utf-8')
        csv_data = pd.read_csv(
            StringIO(csv_data_body), na_filter=False)
        return csv_data
    
    def put_object_to_s3(self, s3_client, bucket_name, save_path, file_name, df ):
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        s3_client.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
        ), Key=f'{save_path}/{file_name.replace(".csv",".csv")}')
        csv_buf.seek(0)

    def process_panel_date(self, panel_date_ls):
        """
        function used to process the extracted date and make it formatted.
        args:
            list: panel_date_ls
        return:
            list: panel date (formatted)
        """
        panel_date = []
        for date_obj in panel_date_ls:
            date = date_obj[0]
            time = date_obj[1] if date_obj[1] else ''
            panel_date.append(f"{date} {time}".strip())
        return panel_date

    def suppress_table_date(self, result_ls, min_max_date):
        return_result_ls = []
        for result in result_ls:
            detected_ls = DateSuppression.date_suppression(
                [(result.get('date'), None, None)], min_max_date)
            if detected_ls:
                result['date'] = detected_ls[0][0]
                return_result_ls.append(result)
            else:
                result['date'] = ""
                return_result_ls.append(result)
        return return_result_ls    
    
    def assign_metadata(self, page, file, section, sub_section,main_section, date_from_tag, extracted_data_list, bb_table, date=None):
        """ 
        the function used to assign the metadata to each result.
        args: 
            int: page
            str: file_name
            str: section
            str: sub_section
            list: extracted_data_list
            str: date
        return:
            list: result list
        """
        res_data_dict_list = []
        isFrom = 'table'
        for res_data_ in extracted_data_list:
            res_data_dict = {}
            res_data_dict['Page'] = page
            res_data_dict['Section'] = section
            res_data_dict['TestName'] = res_data_.get('name')
            res_data_dict['TestResult'] = res_data_.get('text')
            res_data_dict['SubSection'] = sub_section
            res_data_dict['MainSection'] = main_section
            res_data_dict['TestDateTime'] = date if date else res_data_['date']
            res_data_dict['DocumentName'] = file
            res_data_dict['TestUnit'] = None if not res_data_.get(
                'unit') else res_data_.get('unit')
            res_data_dict['DateFrom'] = date_from_tag
            res_data_dict['IsFrom'] = isFrom
            res_data_dict['BBInfo'] = bb_table
            res_data_dict_list.append(res_data_dict)


        return res_data_dict_list

    def get_result(self, lab_metadata_csv_path, sec_subsec_df, min_max_date):
        """
        the function used to generate the structured result from table parser
        args:
            df: pandas df
            str: sec_subsection_file_path (to extract the BB info)
        """
        # sec_subsec_df = pd.read_csv(sec_subsec_file_path, na_filter=False)
        
        tble_mdf = self.read_csv_from_s3(lab_metadata_csv_path)
        table_patterns = {}

        final_result_list = []
        bb_mdata = {}
        for table_name, each_table in self.merged_tables_data.items():
            pages = each_table[0]
            table = each_table[1]
            bb_header = each_table[2]
            bb_table = each_table[3]

            # section, subsection, file_name, page, panel_date
            page_no = pages[0]
            try:
                boundingBoxList = sec_subsec_df[sec_subsec_df[
                    'Page'] == page_no]['Geometry.BoundingBox.Top'].to_list()
            except KeyError as e:
                boundingBoxList = sec_subsec_df[sec_subsec_df[
                    'Page'] == page_no]['Geometry-BoundingBox-Top'].to_list()
            # distance_between_bb = [abs(i-bb_header) for i in boundingBoxList ]
            distance_between_bb = [abs(i-bb_header) for i in boundingBoxList if i and bb_header]
            try:
                line_number_of_nearest_bb = distance_between_bb.index(
                    min(distance_between_bb))
            except ValueError as e:
                line_number_of_nearest_bb = None
                
            # panel_date_ls = []
            nearest_date_ls = []
            is_section_flag = False
            section, main_section, nearest_date_lab_ls = '', '',''
            try:
                file_name = tble_mdf['file_name'].iloc[0].split("/")[-1]
                # section = tble_mdf[tble_mdf['page']
                #                    == page_no]['section'].iloc[0]
                # sub_section = tble_mdf[tble_mdf['page']
                #                        == page_no]['sub_section'].iloc[0]
                if line_number_of_nearest_bb:
                    section = sec_subsec_df[(sec_subsec_df['Page'] == page_no) & (
                        sec_subsec_df['LINE_NUMBER'] == str(line_number_of_nearest_bb))]['section'].values[0]
                    sub_section = sec_subsec_df[(sec_subsec_df['Page'] == page_no) & (
                        sec_subsec_df['LINE_NUMBER'] == str(line_number_of_nearest_bb))]['sub_section'].values[0]
                    main_section = sec_subsec_df[(sec_subsec_df['Page'] == page_no) & (
                        sec_subsec_df['LINE_NUMBER'] == str(line_number_of_nearest_bb))]['updated_main_section'].values[0]
                    nearest_date_lab_string = sec_subsec_df[(sec_subsec_df['Page'] == page_no) & (
                        sec_subsec_df['LINE_NUMBER'] == str(line_number_of_nearest_bb))]['lab_date_dict'].values[0]
                    if nearest_date_lab_string:
                        nearest_date_lab_ls = eval(nearest_date_lab_string)
                # panel_date = tble_mdf[tble_mdf['page']
                #                       == page_no]['panel_date'].iloc[0]
                # panel_date_ls = self.process_panel_date(panel_date)

                # check if the section belong's to ED/DISCH for considering adm/disch date as well
                is_section_flag = True if len(set(section_list).intersection(set([section])))>0 else False
            except IndexError as e:
                pass

            try:
                index_of_nearest_bb = sec_subsec_df[(sec_subsec_df['Page'] == page_no) & (
                    sec_subsec_df['LINE_NUMBER'] == str(line_number_of_nearest_bb))].index[0]
                
                # search through start of the page till we encounter with table (sometimes tag of date get split, hense we are doing this)
                if index_of_nearest_bb:
                    dynamic_search_index_limit = index_of_nearest_bb - sec_subsec_df[sec_subsec_df['Page']
                                                            == page_no].index[0]
                else:
                    dynamic_search_index_limit = 15
                nearest_date_ls = DatetimeExtractor.get_date_time_from_corpus_v2(
                    ' '.join(sec_subsec_df.iloc[index_of_nearest_bb-dynamic_search_index_limit:index_of_nearest_bb+1]['Text'].apply(lambda x: str(x))), date_tags)

                # new change: for lab/result date attachments
                nearest_date = ''
                if main_section and main_section in ['Laboratory']:
                    nearest_detected_lab_date = DateSuppressionLabs.date_suppression_lab(nearest_date_lab_ls, page_no, bb_header)
                    if nearest_detected_lab_date:
                        nearest_date_ls = [nearest_detected_lab_date[0].get(
                            'date')]
                        nearest_date = self.process_panel_date(nearest_date_ls)
                    else:
                        nearest_date = ''
                else:
                    nearest_date_ls = DateSuppression.date_suppression(
                        nearest_date_ls, min_max_date,is_section_flag)
                    nearest_date = self.process_panel_date(nearest_date_ls)
                bb_mdata[bb_header] = {
                    'idx': index_of_nearest_bb, 'dt': nearest_date_ls}
            except Exception as e:
                pass

            # get the extracted value from table
            table_extracted_value_list, pattern = self.table_parser.get_result_from_table_csv(
                table)
            table_patterns[table_name] = pattern
            try:
                if (len(table_extracted_value_list) > 0 and (not self.check_extracted_result_date(table_extracted_value_list)) and (nearest_date )):
                    date_from_tag = 0
                    final_result_list.append(self.assign_metadata(
                        page_no,
                        file_name, section, sub_section,main_section, date_from_tag,
                        # table_extracted_value_list, nearest_date[-1] if nearest_date else panel_date_ls[0]))
                        table_extracted_value_list, bb_table, nearest_date[-1] if nearest_date else ''))
                elif table_extracted_value_list:
                     # suppress the DOS date of extracted result from date present in table
                    try:  
                        table_extracted_value_list = self.suppress_table_date(
                               table_extracted_value_list, min_max_date)
                    except Exception as e:
                        pass
                    date_from_tag = 1
                    final_result_list.append(self.assign_metadata(page_no,
                                                                  file_name, section, sub_section,main_section, date_from_tag,
                                                                  table_extracted_value_list, bb_table))
                else:
                    pass
            except UnboundLocalError as e:
                pass
        final_result_list_ = [ls for ls in final_result_list if len(ls) > 0]
        return final_result_list_, table_patterns

    def save_result(self, final_result_list_, save_path, panel):
        final_df = pd.DataFrame()
        for res_ls in final_result_list_:
            df = pd.DataFrame(res_ls)
            final_df = pd.concat([final_df, df])
        if final_result_list_:
            final_df_ = final_df[['DocumentName', 'Page', 'TestName', 'TestResult', 'TestUnit', 'TestDateTime', 'Section', 'SubSection', 'MainSection',
                                  'DateFrom', 'IsFrom', 'BBInfo']].copy()
            # final_df_.to_csv(
            #     rf"{save_path}\{panel}_{final_df.iloc[0]['DocumentName'].replace('.csv','.csv')}")
            # file_name = f"{panel}_{final_df.iloc[0]['DocumentName'].replace('.csv','.csv')}"
            file_name = f"{panel}_{save_path.split('/')[-1].replace('.csv','')}.csv"
            self.put_object_to_s3(self.s3_client,self.bucket_name,save_path,file_name, final_df_)
            return final_df_
        else:
            return pd.DataFrame({})
