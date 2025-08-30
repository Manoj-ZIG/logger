import re
import PyPDF2
import traceback
import pandas as pd
from io import BytesIO
from datetime import date, datetime

try:
    from helpers.constant import copy_object, delete_object, list_all_objects, read_files, upload_df_to_s3_as_pipe_delimited_txt
except ModuleNotFoundError as e:
    from ..helpers.constant import copy_object, delete_object, list_all_objects, read_files, upload_df_to_s3_as_pipe_delimited_txt

class UpdateManifest():
    def __init__(self, s3c, s3r, bucket_name,document_name_cleaned, pdf_obj, validation_query_df, STATIC_COLS, MANIFEST_FOLDER, MANIFEST_FOLDER_CLEANED, client_doc_pattern_json, adjudication_record_locator):
        self.s3c = s3c
        self.s3r = s3r
        self.bucket_name = bucket_name
        self.document_name_cleaned=document_name_cleaned
        self.pdf_obj = pdf_obj
        self.MAX_PAGE_THRESHOLD = 50
        self.STATIC_COLS = STATIC_COLS
        self.MANIFEST_FOLDER = MANIFEST_FOLDER
        self.MANIFEST_FOLDER_CLEANED = MANIFEST_FOLDER_CLEANED
        self.validation_query_df = validation_query_df
        self.submission_response_adj_list = validation_query_df['adjudication_record_locator'].to_list() if not validation_query_df.empty else []
        self.format_datetime = datetime.now().strftime("%Y%m%d")
        self.medical_record_json = [json for json in client_doc_pattern_json['get_document_info'] if json['document_name'] == 'medical_record'][0]
        self.adjudication_record_locator = adjudication_record_locator

    def read_manifest_files(self, files, pdf_name_map):
        """
        ### Reads the manifest file of a particular document

        args:
            bucket_name: bucket name
            files: list
            pdf_name_map: dict
        return:
            dfs: list
        """
        dfs = []
        for f in files:
            if f.endswith('.txt'):
                response = self.s3c.get_object(Bucket = self.bucket_name, Key=f)
                data = response['Body'].read()
                df = pd.read_csv(BytesIO(data), sep='|')
                if len(df.columns) == 1:
                    df = pd.read_csv(BytesIO(data), delimiter='\t')
                df['zigna_s3_path'] = f
                df['zip_file_name'] = f.split('/')[-1].replace('.txt', '')
                if 'PDF_Name' in list(df.columns):
                    df['PDF_Name'] = df['PDF_Name'].apply(lambda x: pdf_name_map.get(x,x) )
                    df['total_files_received'] = len(set(df['PDF_Name']))
                else:
                    df['total_files_received'] = 'NA'
                df['Adjudication Record Locator'] = df['Adjudication Record Locator'].astype(str)
                dfs.append(df)
        return dfs

    def is_selected_check(self, row):
        """
        ### This function updates the row in a dataframe

        args:
            row: Series
        return:
            p: int
        """
        # global submission_response_adj_list
        adj_num = self.adjudication_record_locator
        if adj_num in self.submission_response_adj_list:
            p = 1
        else:
            p = 0
        return p

    def add_cleaned_columns(self, df):
        # self.medical_record_json['pattern']
        pattern = re.compile(self.medical_record_json['pattern'])
        cleaned_df = df.copy()
        cleaned_df['cleaned_vendor_code'] = cleaned_df['Vendor Code']
        cleaned_df['is_vendor_audit_id_missing'] = cleaned_df['Vendor Audit ID'].apply(lambda x: 0 if x else 1)
        # cleaned_df['cleaned_adjudication_record_locator'] = cleaned_df.apply(lambda row: row['Adjudication Record Locator'] if str(row['Adjudication Record Locator']).startswith('AJX') else row['Vendor Audit ID'], axis=1)
        # cleaned_df['is_zigna_adjudication_id'] = cleaned_df.apply(lambda row: 1 if str(row['Adjudication Record Locator']).startswith('AJX') else 0, axis=1)
        cleaned_df['cleaned_adjudication_record_locator'] = cleaned_df.apply(lambda row: row['Adjudication Record Locator'] if re.findall(pattern, str(row['Adjudication Record Locator'])) else row['Vendor Audit ID'], axis=1)
        cleaned_df['is_zigna_adjudication_id'] = cleaned_df.apply(lambda row: 1 if re.findall(pattern, str(row['Adjudication Record Locator'])) else 0, axis=1)
        cleaned_df['is_selected'] = cleaned_df.apply(self.is_selected_check,axis = 1)
        cleaned_df['pdf_adjudication_record_locator'] = self.adjudication_record_locator

        for i,j in cleaned_df.iterrows():
            try:
                print('pdf path',j['zigna_s3_path'])
                pdf_path = j['zigna_s3_path'].split('/')
                pdf_path[-1] = j['PDF_Name'] if j['PDF_Name'].endswith(".pdf") else j['PDF_Name']+".pdf"
                pdf_path[-2] = pdf_path[-2].replace('manifest','medical_records')
                cleaned_df.at[i,'pdf_s3_path'] =  '/'.join(pdf_path)
                response = self.s3c.get_object(Bucket= self.bucket_name, Key=cleaned_df.at[i,'pdf_s3_path'])
                pdf_content = response['Body'].read()

                # Create a file-like object from the downloaded content
                pdf_file = BytesIO(pdf_content)

                # Read the text from the PDF using PyPDF2
                print(f"digitization check started:- {self.document_name_cleaned}")
                is_digitised, is_corrupted, num_pages  = self.pdf_obj.digitise_check(pdf_file)
                print(f"digitization check compelted:- {self.document_name_cleaned}")

                cleaned_df.at[i,'is_digitised'] = int(is_digitised)
                cleaned_df.at[i,'is_corrupted'] = int(is_corrupted)
                cleaned_df.at[i,'num_pages'] = int(num_pages)
                df.at[i,'num_pages'] =int(num_pages)
            except:
                print(traceback.format_exc())
                print(f"Exception occured for {j['zigna_s3_path']}")
                cleaned_df.at[i,'is_digitised'] = -1
                cleaned_df.at[i,'is_corrupted'] = -1
                cleaned_df.at[i,'num_pages'] = -1
        return cleaned_df

    def pdf_to_raw_files(self, df):
        print("ENTERED PDF TO RAW FILES")
        updated_pdf_paths = {}
        df1 = df[
        (df['adjudication_record_locator'] == df['zai_pdf_adjudication_record_locator']) &
        (df['zai_is_selected_by_client'] == 1) &
        (df['zai_is_corrupted'] == 0)]

        output_folder = 'mr_data_pipeline/raw/'
        default_prefix = 'mr_data_pipeline/'
        current_date =  datetime.now().strftime("%Y%m%d")
        try :
            status_file_prefix = default_prefix + 'status_file_folder/medical_record_processing_status'
            file_key = status_file_prefix + f"06_mr_processed_{current_date}_logs.txt"
            status_resp = self.s3c.get_object(Bucket = self.bucket_name,Key = file_key)
            file_content = status_resp['Body'].read().decode('utf-8')
        except:
            file_content = ''
        for j,i in df1.iterrows():
            pdf_path = i['zigna_s3_path'].split('/')
            pdf_path[-1] = i['pdf_name'] if i['pdf_name'].endswith(".pdf") else i['pdf_name']+".pdf"
            pdf_path[-2] = pdf_path[-2].replace('manifest','medical_records')
            pdf_path_input = '/'.join(pdf_path)
            if i['pdf_name'].replace(".pdf","") not in file_content:
                output_bucket = "zai-revmax-qa"
                try:
                    print(f"checking claim_id existence:- {self.document_name_cleaned}")
                    client_claim_id = self.validation_query_df.iloc[0]['claim_id']
                    print(f"claim_id found:- {self.document_name_cleaned}")


                except:
                    print(f"claim_id not found:- {self.document_name_cleaned}")

                    client_claim_id = ''
                print(f"Checking if document is greater than 75 pages:- {self.document_name_cleaned}")
                if i['zai_num_pages']>75:
                    print(f"Document is greater than 75 pages:- {self.document_name_cleaned}")

                    if i['is_arl_matched'] == 1:
                        adj = i['pdf_name'].split('_')[0]
                        output_pdf_name = output_folder + client_claim_id +'_' +pdf_path[-1]
                        print("CLIENT ID ",client_claim_id)
                        print("OUTPDF NAME ", output_pdf_name)
                        if len(client_claim_id.strip())>0:
                            print(f'PDF chunking started:- {self.document_name_cleaned}')
                            chunk_paths = self.pdf_obj.chunk_raw_pdf(self.bucket_name,pdf_path_input,output_bucket,output_pdf_name)
                            print(f'PDF chunking completed:- {self.document_name_cleaned}')

                            # delete_object(self.s3c, self.bucket_name, pdf_path_input)
                        else:
                            output_pdf_name = 'mr_data_pipeline/claim_not_found/' + client_claim_id +'_' +pdf_path[-1]
                            chunk_paths = self.pdf_obj.chunk_raw_pdf(self.bucket_name,pdf_path_input,output_bucket,output_pdf_name)
                            # delete_object(self.s3c, self.bucket_name, pdf_path_input)
                        if chunk_paths:
                            updated_pdf_paths[i['pdf_name']] = chunk_paths
                            
                else:
                    print(f"Document is less than 75 pages:- {self.document_name_cleaned}")

                    output_pdf_name = 'mr_data_pipeline/manual_review/'+ client_claim_id +'_' +pdf_path[-1]

                    print(f"Moving pdf to manual review QA:- {self.document_name_cleaned}")
                    copy_object(self.s3c, self.bucket_name, pdf_path_input, output_bucket, output_pdf_name)
                    print(f"Moved pdf to manual review QA:- {self.document_name_cleaned}")
                    

        pdf_chunks = [i[0].split('/')[-1] for i in list(list(updated_pdf_paths.values())[0])]
        print(f"Chunks Generated: {len(pdf_chunks)}:- {self.document_name_cleaned}") 
        return updated_pdf_paths

    def update_chunk_info(self, df,updated_pdf_paths):
        """
        ### This function updates the pdf name and page count and is chunked by zigna

        args:
            updated_pdf_paths: 
        return:
            df
        """
        print("Entered updating chunked pdf info in cleaned manifest")
        new_rows = []
        for pdf_name, chunks in updated_pdf_paths.items():
            matching_rows = df[df['pdf_name'] == pdf_name]
            if not matching_rows.empty:
                matching_row = matching_rows.iloc[0].to_dict()
                df = df[df['pdf_name']!=pdf_name]
                for chunk,pages in chunks:
                    new_row = matching_row.copy()
                    pdfname = chunk.split('/')[-1]
                    claimid = pdfname.split('_')[0]
                    new_row['pdf_name'] = pdfname.replace(claimid+'_','')
                    new_row['zai_num_pages'] = int(pages)
                    new_row['is_zigna_chunked'] = int(1)
                    new_rows.append(new_row)
        df = pd.concat([df,pd.DataFrame(new_rows)],ignore_index = True)
        return df

    def upload_manifest(self, df, folder_type, n, pdf_file_name, document_name_cleaned):
        """
        ### This function uploads the manifest file

        args:
            df: DataFrame
            folder_type: str
            n: int
            pdf_file_name: str (pdf_file_name)
        return:
            None
        """
        formatted_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
        cleaned_df  = self.add_cleaned_columns(df)
        cleaned_df['num_pages'] = cleaned_df['num_pages'].astype(int)
        cleaned_df['is_corrupted'] = cleaned_df['is_corrupted'].astype(int)
        cleaned_df['is_digitised'] = cleaned_df['is_digitised'].astype(int)
        cleaned_df['is_vendor_audit_id_missing'] = cleaned_df['is_vendor_audit_id_missing'].astype(int)
        cleaned_df['is_selected'] = cleaned_df['is_selected'].astype(int)
        cleaned_df['is_zigna_adjudication_id'] = cleaned_df['is_zigna_adjudication_id'].astype(int)
        cleaned_df.rename(columns={'total_files_received':'zai_total_files_received','Vendor Code':'vendor_code','Vendor Audit ID':'vendor_audit_id','Client Audit ID':'client_audit_id','Adjudication Record Locator':'adjudication_record_locator',
                                'Member Record Locator':'member_record_locator','Document Type Code':'document_type_code','Document Rec Date':'document_rec_date','Action Date':'action_date','PDF_Name':'pdf_name',
                                'is_zigna_manifest':'zai_manifest','load_date':'zai_load_date','cleaned_vendor_code':'zai_cleaned_vendor_code','is_vendor_audit_id_missing':'zai_is_vendor_audit_id_missing',
                                'cleaned_adjudication_record_locator':'zai_cleaned_adjudication_record_locator','is_zigna_adjudication_id':'zai_is_zigna_adjudication_id','is_selected':'zai_is_selected_by_client',
                                'pdf_adjudication_record_locator':'zai_pdf_adjudication_record_locator','pdf_s3_path':'zai_pdf_s3_path','is_digitised':'zai_is_digitised','is_corrupted':'zai_is_corrupted','num_pages':'zai_num_pages' },inplace=True)
        for i in df.columns:
            df[i]=df[i].fillna('').astype(str)
        df.rename(columns={},inplace = True)
        filename=f'{self.MANIFEST_FOLDER}/{folder_type}/{str(date.today()).replace("-","")}/{pdf_file_name.replace(".pdf", "")}_{formatted_datetime}_{n}_raw_manifest_file.txt'
        print(f'Txt manifest file uploading:- {self.document_name_cleaned}')
        upload_df_to_s3_as_pipe_delimited_txt(self.s3c, df, self.bucket_name, filename)
        print(f'Txt manifest file uploaded:- {self.document_name_cleaned}')

        print(f"EC2 : file_uploaded {filename}")
        updated_pdf_paths = self.pdf_to_raw_files(cleaned_df)
        cleaned_df = self.update_chunk_info(cleaned_df,updated_pdf_paths)
        if folder_type == 'processed':
            # file_name = f'{self.MANIFEST_FOLDER_CLEANED}/cleaned_updated_raw_manifest_file.parquet'
            try:
                # dcument_type = '.txt'
                # keys = list_all_objects(self.s3r, self.bucket_name, self.MANIFEST_FOLDER_CLEANED, dcument_type)
                # df_1 = read_files(self.s3c, self.bucket_name, keys)
                # print(f"found existing cleaned manifest , so concatinating")
                # result = pd.concat([df_1,cleaned_df])
                # result.reset_index(drop=True,inplace=True)
                result = cleaned_df.copy()
            except self.s3c.exceptions.NoSuchKey as e:
                print('cant find cleaned updated manifest so creating one!', e)
                # result = cleaned_df.copy()
            for i in result.columns:
                result[i]=result[i].fillna('').astype(str)
            manifest_file_path = self.MANIFEST_FOLDER_CLEANED + "/" + document_name_cleaned.replace(".pdf", "")+'.txt'
            upload_df_to_s3_as_pipe_delimited_txt(self.s3c, result, self.bucket_name, manifest_file_path)

    def cleanup(self, manifest_df,zip_file_name,is_zigna_manifest, document_name_cleaned, is_arl_present,is_arl_received,is_arl_matched ):
        """
        ### This function updates the manifest file in the required format

        args:
            manifest_df: list[dataframe] 
            zip_file_name: str 
            is_zigna_manifest: int 
            is_arl_present: int
            is_arl_received: int
            is_arl_received: int
        return:
            None
        """
        print("entered clean up ")

        for n, df in enumerate(manifest_df):
            # print(type(df))
            if (len(set(df.columns).intersection(set(self.STATIC_COLS)))==12) and (len(df.columns) == 12):
                df['is_zigna_manifest'] = is_zigna_manifest
                df['load_date'] = self.format_datetime
                df['is_arl_present'] = is_arl_present
                df['is_arl_received'] = is_arl_received
                df['is_arl_matched'] = is_arl_matched
                self.upload_manifest(df,'processed',n,zip_file_name, document_name_cleaned)
            else:
                df['is_arl_present'] = is_arl_present
                df['is_arl_received'] = is_arl_received
                df['is_arl_matched'] = is_arl_matched
                df['load_date'] = self.format_datetime
                self.upload_manifest(df,'error',n,zip_file_name, document_name_cleaned)
    
    def update_manifest(self, manifest_files, pdf_name_map, document_name_cleaned, zip_folder_name, is_zigna_manifest, \
                        is_arl_present_in_document_name, is_arl_received, is_arl_matched_with_document):
        dfs = self.read_manifest_files(manifest_files, pdf_name_map)
        self.cleanup(dfs, zip_folder_name, is_zigna_manifest, document_name_cleaned, is_arl_present_in_document_name, is_arl_received, is_arl_matched_with_document)