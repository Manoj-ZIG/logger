import os
import re
import boto3
import logging
import numpy as np
import pandas as pd
from io import StringIO
try:
    from constant.date_tag_constant import demographics_ls
    from constant.section_constant_terms import map_constant
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
except:
    from ..constant.date_tag_constant import demographics_ls
    from ..constant.section_constant_terms import map_constant
    from ..constant.aws_config import aws_access_key_id, aws_secret_access_key

try:
    from utils import read_csv_file
except ModuleNotFoundError as e:
    from .utils import read_csv_file

class IndexPage:
    def __init__(self, file_name, bucket_name, section_subsection_constant):
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.demogrphics = section_subsection_constant['demogrphics']['demographics_ls']
        self.map_constant = section_subsection_constant['section_constant_terms']['map_constant']
        self.parameters_bucket_name = os.environ["PARAMETERS_BUCKET_NAME"]

    def IndexPage(self, path_to_save_result, df_co_date_order, df, df_co):
        logging.info('Adding an index page with hyperlinks associated with it')
        File_name = self.file_name.replace(".csv", '')
        df_co_index = pd.DataFrame({"chart_order": ['Demographics (DEMO)',
                                    'Emergency Department (ED)',
                                                    'History and Physical (H&P)',
                                                    'Progress Notes including Consults (Physician/QHP)',
                                                    'Operative/Procedure Note',
                                                    'Discharge Summary (DS)',
                                                    'Therapy Notes',
                                                    'Dietary/Nutritional Notes',
                                                    'Nursing Documentation',
                                                    'LABS',
                                                    'Imaging',
                                                    'Orders',
                                                    'Miscellaneous'],
                                    })
        self.s3_c = boto3.client('s3', region_name='us-east-1')
        zai_main_section_map_params_key = os.environ["ZAI_MAIN_SECTION_MAP_PARAMS"]
        print(f"Reading zai_main_section_map_params -'{self.parameters_bucket_name}' - '{zai_main_section_map_params_key}'")
        chart_order_df = read_csv_file(self.s3_c, self.parameters_bucket_name, zai_main_section_map_params_key, encoding='latin')

        chart_order_df['chart_order'] = chart_order_df['chart_order'].mask(
            chart_order_df['chart_order'] == np.NaN, None).ffill()

        df_co_date_order = df_co_date_order.reset_index()
        df_co_date_order['chart_order'] = np.NaN
        for i in range(len(df_co_date_order)):
            # entity = df_co_date_order.iloc[i]['main_section']
            entity = df_co_date_order.iloc[i]['main_section_actual']
            try:
                # chart_name = chart_order_df[chart_order_df['main_section']
                #                      == entity]['chart_order'].values[0]
                # df_co_date_order.at[i, 'chart_order'] = chart_name
                chart_name = chart_order_df[chart_order_df['main_section']
                                     == entity]['chart_order'].values[0]
                df_co_date_order.at[i, 'chart_order'] = chart_name
            except:
                pass

        df_order_entity = pd.DataFrame()
        df_co_date_order['main_section'] = df_co_date_order['main_section'].fillna(np.NaN)
        df_co_date_order['main_section'] = df_co_date_order['main_section'].replace(np.NaN, 'nan')
        # df_order_entity = df_co_date_order[(df_co_date_order['range'] != 'nan') & (df_co_date_order['main_section'] != 'nan')].sort_values(
        #     by=['chart_order', 'group_date'])
        df_co_date_order['main_section_actual'] = df_co_date_order['main_section_actual'].fillna(np.NaN)
        df_co_date_order['main_section_actual'] = df_co_date_order['main_section_actual'].replace(np.NaN, 'nan')
        df_order_entity = df_co_date_order[(df_co_date_order['range'] != 'nan') & (df_co_date_order['main_section_actual'] != 'nan')].sort_values(
            by=['chart_order', 'group_date'])
        Zigna_claim_key = File_name.split('_')[0]
        def make_clickable(page, name):
            if page != '':
                return '<a href="{}_highlighted.pdf#page={}" rel="noopener noreferrer" target="_blank">{}</a>'.format(File_name.replace(Zigna_claim_key + '_',''), eval(page)[0], name)
            else:
                return '{}'.format(name)
        df_order_entity['group_date'] = df_order_entity['group_date'].apply(lambda x: "" if str(x) == 'NaT' else x)
        if df_order_entity.shape[0] > 0:
            # df_order_entity['entity'] = df_order_entity.apply(lambda x: make_clickable(x['range'], df_CO[df_CO['Group'] == x['Group']].iloc[0]['entity']), axis=1)
            # df_order_entity['entity_actual'] = df_order_entity.apply(lambda x: make_clickable(x['range'], df_CO[df_CO['Group'] == x['Group']].iloc[0]['entity']), axis=1)
            df_order_entity['entity_actual'] = df_order_entity['entity_actual'].replace(self.map_constant)
            df_order_entity['entity_actual'] = df_order_entity.apply(lambda x: make_clickable(x['range'], x['entity_actual']), axis=1)
        index_page_str = ""
        # Logic for demographics
        ls = []
        df_demo = pd.DataFrame({})
        df_demo_ = pd.DataFrame({})
        df_mis = pd.DataFrame({})
        df_mis_ = pd.DataFrame({})
        flag = 0
        pattern1 = "demographics|patient demographics|demographic information|demographic info"
        corpus = ' '.join(df['Text'].apply(lambda x: str(x)).to_list())
        match_obj = re.finditer(pattern1, corpus, re.I)
        match_obj_count = 0
        for each_match in match_obj:
            if df[(df['Start'] <= each_match.start())].iloc[-1]['Page'] == df[(df['End'] >= each_match.end())].iloc[1]['Page']:
                page = df[(df['Start'] <= each_match.start())].iloc[-1]['Page']
                pattern2 = "|".join(self.demogrphics)
                if len(set(re.findall(rf"({pattern2})", " ".join(df[df['Page'] == page]['Text'].apply(lambda x: str(x))).lower()))) >= 4:
                    ls.append(['Demographics (DEMO)',
                              f'{File_name}', f'({page},{page})'])
                    match_obj_count += 1
                    flag = 1
            if match_obj_count == 3:
                break

        if flag == 1:
            df_demo = pd.DataFrame(
                ls, columns=['chart_order', 'file_name', 'range'])
            df_demo_ = df_demo.copy()
            df_demo_['entity_actual'] = 'Patient Demographics'
            df_demo['chart_order'] = df_demo.apply(lambda x: make_clickable(x['range'], x['chart_order']), axis=1)
            html = df_demo[['chart_order', 'range']].reset_index().drop(columns={'index'}).rename(columns={'range': 'Page Range'}).to_html(render_links=True, escape=False)
            index_page_str += "<center><strong>" + 'Demographics (DEMO)' + "</strong> </center><br>\n"
            index_page_str += "<center>" + html + "</center><br><br><br>\n"

        if flag == 0:
            pattern = "|".join(self.demogrphics)
            page_ls = round(df['Page'].max()*0.05)
            for i in range(1, page_ls):
                if len(set(re.findall(rf"({pattern})", " ".join(df[df['Page'] == i]['Text'].apply(lambda x: str(x))).lower()))) > 4:
                    index_page_str += "<center><strong>" + 'Demographics (DEMO)' + "</strong> </center><br>\n"
                    df_demo = pd.DataFrame([['Demographics (DEMO)', f'{File_name}', f'({i},{i})']], columns=['chart_order', 'file_name', 'range'])
                    df_demo_ = df_demo.copy()
                    df_demo_['entity_actual'] = 'Patient Demographics'
                    df_demo['chart_order'] = df_demo.apply(lambda x: make_clickable(x['range'], x['chart_order']), axis=1)
                    html = df_demo[['chart_order', 'range']].reset_index().drop(columns={'index'}).rename(columns={'range': 'Page Range'}).to_html(render_links=True, escape=False)
                    index_page_str += "<center>" + html + "</center><br><br><br>\n"
                    break

        if df_order_entity.shape[0] > 0:
            for i in df_co_index['chart_order']:
                if len(df_order_entity[df_order_entity['chart_order'] == i]):
                    index_page_str += "<center><strong>" + i + "</strong> </center><br>\n"
                    html = df_order_entity[df_order_entity['chart_order'] == i][['entity_actual', 'group_date', 'range', '_Score_']].reset_index().drop(
                        columns={'index'}).rename(columns={'entity_actual': 'Section Name', 'range': 'Page Range'}).to_html(render_links=True, escape=False)
                    index_page_str += "<center>" + html + "</center><br><br><br>\n\n"
                    print(i)

        # Miscellaneous----------------------------------------------------------------
        page_range_ls = df_order_entity['range'].to_list()

        pages_range = set([i for i in range(1, df.iloc[-1]['Page']+1)])
        for i in page_range_ls:
            pages_range = pages_range.difference(
                set([j for j in range(eval(i)[0], eval(i)[1]+1)]))
        pages_range = list(pages_range)

        if pages_range:
            ls = pages_range
            prev = ls[0]
            mis_page_ls = []
            for i in range(1, len(ls)):
                if ls[i-1]+1 == ls[i]:
                    continue
                mis_page_ls.append(f'({prev},{ls[i-1]})')
                prev = ls[i]
            mis_page_ls.append(f'({prev},{ls[-1]})')

            df_mis = pd.DataFrame(mis_page_ls).rename(columns={0: 'range'})
            df_mis['chart_order'] = 'Miscellaneous'
            df_mis['file_name'] = f'{File_name}'
            if ls[-1] == df.iloc[-1]['Page']:
                df_mis['entity_actual'] = 'Miscellaneous'
            df_mis_ = df_mis.copy()
            df_mis['chart_order'] = df_mis.apply(
                lambda x: make_clickable(x['range'], x['chart_order']), axis=1)
            index_page_str += "<center><strong>" + \
                "Miscellaneous" + "</strong> </center><br>\n"
            html = df_mis[['chart_order', 'range']].reset_index().drop(columns={'index'}).rename(
                columns={'range': 'Page Range'}).to_html(render_links=True, escape=False)
            index_page_str += "<center>" + html + "</center><br><br><br>\n\n"
        zigna_claim_key = File_name.split("_")[0]
        copy_key = f'sample_client/audits/medical_records/{zigna_claim_key}/{self.file_name.replace(".csv","")}_index.html'
        self.s3_c.put_object(Bucket=self.bucket_name, Body=index_page_str,
                             Key=f'{path_to_save_result}/{self.file_name.replace(".csv","")}_index.html')
        # self.s3_c.copy_object(CopySource=f'{path_to_save_result}/{self.file_name.replace(".csv","")}_index.html', Bucket='zai-revmax-test',
        #                 Key=copy_key)
        # --------------- local ---------------
        # with open(rf"{path_to_save_result}\{File_name}_index.html", "w") as f:
        #     f.write(index_page_str)
        # --------------- end ---------------
        logging.info('Converting DataFrame to html')
        html = df_order_entity.to_html(render_links=True, escape=False)

        self.df_co_date_order = df_co_date_order
        self.df_chart_view = self.df_co_date_order[(self.df_co_date_order['range'] != 'nan') & (df_co_date_order['main_section'] != 'nan')].sort_values(by=['chart_order', 'group_date'])
        self.df_chart_view = self.df_co_date_order[(self.df_co_date_order['range'] != 'nan') & (df_co_date_order['main_section_actual'] != 'nan')].sort_values(by=['chart_order', 'group_date'])
        columns_ = self.df_chart_view.columns.to_list()
        if len(df_demo) > 0:
            self.df_chart_view = pd.concat([df_demo_, self.df_chart_view])
            self.df_chart_view = self.df_chart_view[columns_]
            self.df_chart_view = self.df_chart_view.fillna('')
        if len(df_mis) > 0:
            self.df_chart_view = pd.concat([self.df_chart_view, df_mis_])
            self.df_chart_view = self.df_chart_view.fillna('')

        self.df_chart_view['group_date'] = self.df_chart_view['group_date'].apply(lambda x: "" if str(x) == 'NaT' else x)
        
        
        # --------------- lambda ---------------
        csv_buf = StringIO()
        self.df_chart_view.to_csv(csv_buf, header=True, index=False)
        self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
        ), Key=f'{path_to_save_result}/{self.file_name.replace(".csv","")}_chart_view.csv')
        csv_buf.seek(0)

        # csv_buf = StringIO()
        # self.df_co_date_order.to_csv(csv_buf, header=True, index=False)
        # self.s3_c.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(
        # ), Key=f'{path_to_save_result}/section_date_order_{self.file_name.replace(".csv",".csv")}')
        # csv_buf.seek(0)
        # --------------- end ---------------
