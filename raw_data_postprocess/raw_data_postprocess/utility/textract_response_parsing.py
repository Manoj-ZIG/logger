import pandas as pd
import numpy as np
import boto3
from io import StringIO
import os
import time


from trp import Document
import itertools
import string


class TextractTableResponse:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def put_object_to_s3(self, df,bucket_name, save_path, file_name):
        csv_buf = StringIO()
        df.to_csv(csv_buf, index=False)
        self.s3_client.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
        ), Key=f'{save_path}/{file_name.replace(".csv",".csv")}')
        csv_buf.seek(0)
    
    def get_textract_table_csv(self, response, file_name, page_list, bucket_name, save_path):
        """  
        function helps to store the csv files from detected table, 
        fileName format: pageNo_alpha_scoreOfTable_docName.csv
        args: 
            list: textract response
            str: file_name
            list: page_list
            str: save_path
        return:
            dict: table_meta_data (key: table_file_name,
                                   value: boundingBox top)

        """
        page_list = sorted(list(set(page_list)))
        letters = list(
            itertools.chain(
                string.ascii_uppercase,
                (''.join(pair)
                    for pair in itertools.product(string.ascii_uppercase, repeat=2))
            ))

        doc = Document(response)
        table_data = []
        table_meta_data = {}
        for page_idx, page in enumerate(doc.pages):
            page_tables = []
            for idx, table in enumerate(page.tables):
                tbl_cell_data = []
                # try:
                #     bb_header = table.rows[0].cells[0].geometry.boundingBox.top
                # except Exception as e:
                #     pass
                try:
                    bb_top_ele = table.rows[0].cells[0].geometry.boundingBox.top
                    bb_top = table.geometry.boundingBox.top
                    bb_left = table.geometry.boundingBox.left
                    bb_height = table.geometry.boundingBox.height
                    bb_width = table.geometry.boundingBox.width
                except Exception as e:
                    bb_top, bb_left, bb_height, bb_width = None, None, None, None

                table_confidence = []
                for row in table.rows:
                    row_data = [cell.text for cell in row.cells]
                    table_confidence.append(np.average(
                        np.array([cell.confidence for cell in row.cells])))
                    tbl_cell_data.append(row_data)
                page_tables.append(tbl_cell_data)

                # save the csv file
                score_ = int(np.average(np.array(table_confidence)))
                file_name_ = file_name.split("/")[-1].replace(".pdf", "").replace(".csv","")
                table_file_name = f"{page_list[page_idx]}_{letters[idx]}_{score_}_{file_name_}.csv"
                self.put_object_to_s3(pd.DataFrame(tbl_cell_data),
                                      bucket_name, rf"{save_path}", table_file_name)
               
                table_meta_data[str(table_file_name)] = tuple([bb_top_ele, (bb_top, bb_left, bb_height, bb_width)])
            table_data.append(page_tables)
        return table_meta_data
