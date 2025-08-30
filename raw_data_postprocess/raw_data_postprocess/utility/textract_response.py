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
    def __init__(self, textract_client, s3_client):
        self.textract_client = textract_client
        self.s3_client = s3_client

    def get_result(self, jobId):
        maxResults = 1000
        paginationToken = None
        finished = False
        pages = []

        while finished == False:
            response = None
            if paginationToken == None:
                response = self.textract_client.get_document_analysis(
                    JobId=jobId, MaxResults=maxResults)
            else:
                response = self.textract_client.get_document_analysis(
                    JobId=jobId, MaxResults=maxResults, NextToken=paginationToken)
            pages.append(response)
            print('Document Detected.')
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True

        return pages

    def extract_text(self, bucket_name, file_name):
        '''
        Extracts tables from a pdf file stored in s3 bucket
        args:
            bucket_name: string
            file_name: string
        returns:
            response: dict
        '''
        # Call Amazon Textract
        response = self.textract_client.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': file_name
                }
            },
            FeatureTypes=['TABLES']
        )

        # Get the JobId from the response
        job_id = response['JobId']

        # Check the status of the Textract job
        while True:
            job_status = self.textract_client.get_document_analysis(JobId=job_id)[
                'JobStatus']
            if job_status in ['SUCCEEDED', 'FAILED']:
                break
            print('.', end=' ')
            time.sleep(3)  # Wait for a few seconds before checking again

        if job_status == 'SUCCEEDED':
            response_pages = self.get_result(job_id)

        return response_pages

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
        # if os.path.exists(save_path):
        #     os.rmdir(save_path)
        # os.makedirs(save_path)
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
                try:
                    bb_header = table.rows[0].cells[0].geometry.boundingBox.top
                except Exception as e:
                    pass

                table_confidence = []
                for row in table.rows:
                    row_data = [cell.text for cell in row.cells]
                    table_confidence.append(np.average(
                        np.array([cell.confidence for cell in row.cells])))
                    tbl_cell_data.append(row_data)
                page_tables.append(tbl_cell_data)

                # save the csv file
                score_ = int(np.average(np.array(table_confidence)))
                file_name_ = file_name.split("/")[-1].replace(".pdf", ".csv")
                table_file_name = f"{page_list[page_idx]}_{letters[idx]}_{score_}_{file_name_}"
                # pd.DataFrame(tbl_cell_data).to_csv(
                #     os.path.join(save_path, table_file_name), index=False)
                self.put_object_to_s3(pd.DataFrame(tbl_cell_data),
                                      bucket_name, rf"{save_path}/table_csv", table_file_name)
               
                table_meta_data[str(table_file_name)] = bb_header
            table_data.append(page_tables)
        return table_meta_data
