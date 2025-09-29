import re
# import fitz
import PyPDF2
import pandas as pd
from datetime import datetime
from io import BytesIO, StringIO
from dateutil.parser import parse
from io import BytesIO,BufferedReader

try:
    from helpers.received_file_logs import ReceivedFileLogs
    from helpers.constant import list_all_objects, read_files
    from helpers.generate_validation_query import get_all_pcn_and_arl
except ModuleNotFoundError as e:
    from ..helpers.received_file_logs import ReceivedFileLogs
    from ..helpers.constant import list_all_objects, read_files
    from ..helpers.generate_validation_query import get_all_pcn_and_arl

class DupicateCheck:
    def __init__(self, s3c, s3r, bucket_name, key, client_name, document_name, document_type, response, received_file_logs_key, \
                 received_file_logs_path, received_file_logs_obj, document_manual_review_path, medical_record_manual_review_path, \
                    duplicate_document_path, duplicate_medical_record_path):
        self.s3c = s3c
        self.s3r = s3r
        self.key = key
        self.response = response
        self.doc_name = document_name
        self.doc_type = document_type
        self.client_name = client_name
        self.bucket_name = bucket_name
        self.received_file_logs_path = received_file_logs_path
        self.received_file_logs_key = received_file_logs_key
        self.received_file_logs_obj = received_file_logs_obj
        self.document_manual_review_path = document_manual_review_path
        self.medical_record_manual_review_path = medical_record_manual_review_path
    
        self.duplicate_document_path = duplicate_document_path
        self.duplicate_medical_record_path = duplicate_medical_record_path

    def get_page_count(self, text, doc_name):
        '''
        ### Reads the pdf and returns the number of pages
        args:
            doc_name: pdf document name
        return:
            None
        '''
        path = "/".join(f"{self.key}".split("/")[:-1:])
        pdf_doc_path = f"{path}/" + doc_name
        response = self.s3c.get_object(Bucket=self.bucket_name, Key=pdf_doc_path)
        fs = response['Body'].read()
        size = response['ContentLength']
        pdfFile = PyPDF2.PdfReader(BufferedReader(BytesIO(fs)))
        no_of_pages = len(pdfFile.pages)
        print(f"The number of pages for the MR({text}) : '{doc_name}' - {no_of_pages}")
        return no_of_pages

    def sort_dict_by_nearest_value_and_name(self, pdf_dict, target_pages, target_name):
        '''
        ### Sort the dictionary by the absolute difference between the values and the target number
        
        args:
            pdf_dict : dict ({MRName : page_count})
            target_pages : int (current doc page count)
            target_name : str (current pdf name)
        return:
            status : True/False
            processed_doc_name : processed document name
            processed_doc_size : size of MR
        '''
        # Sort the dictionary by the absolute difference between the values and the target number
        sorted_dict = dict(sorted(pdf_dict.items(), key=lambda item: (abs(item[1] - target_pages), abs(len(item[0]) - len(target_name)))))
        return sorted_dict

    def check_file_processed(self, df_arl):
        '''
        ### Checks if the current document is present in the received file logs
        
        args:
            None
        return:
            status : True/False
            processed_doc_name : processed document name
            processed_doc_size : size of MR
        '''
        try:
            print(f"Reading received file logs to check if the document '{self.doc_name}' is processed")
            # Step-1 Reading received file logs
            dcument_type = '.txt'
            keys = list_all_objects(self.s3r, self.bucket_name, self.received_file_logs_path, dcument_type)
            df = read_files(self.s3c, self.bucket_name, keys)
            if self.doc_type.lower() == 'pdf':
                if df_arl.empty:
                    return False, None, None
                pcn = df_arl['adjudication_record_locator'].to_list()
                pattern = '|'.join(pcn)
                filtered_df = df[df['FILENAME'].str.upper().str.contains(pattern, regex=True)]

                if filtered_df.empty:
                    return False, None, None
                else:
                    pdf_dict = {}
                    for idx, row in filtered_df.iterrows():
                        try:
                            page_count = self.get_page_count("processed document", row['FILENAME'])
                        except:
                            page_count = -1
                        pdf_dict[row['FILENAME']] = page_count
                    curr_doc_page_count = self.get_page_count("current document", self.doc_name)
                    sorted_pdf_dict = self.sort_dict_by_nearest_value_and_name(pdf_dict, curr_doc_page_count, self.doc_name)
                    possible_duplicate_pdf = next(iter(sorted_pdf_dict.items()))
                    return True, possible_duplicate_pdf[0], int(filtered_df[filtered_df['FILENAME'] == possible_duplicate_pdf[0]].iloc[0]['SIZE'])
            else:
                # FILENAME|RECEIVED_DATE|STATUS|SIZE|ZIGNA_FILE_NAME
                # Step-2 Split the content into lines and process each line
                for idx, row in df.iterrows():
                    processed_doc_name = row['FILENAME']
                    processed_doc_size = int(row['SIZE'])
                    if self.doc_name.upper() == processed_doc_name.upper():
                        return True, processed_doc_name, processed_doc_size 
                #If no match is found, return False and None       
                return False, None, None
        except self.s3r.meta.client.exceptions.NoSuchKey:
            return False, None, None
            
    
        except Exception as e:
            print(f"An error occurred while checking if the file was processed: {e}")
            print(f"duplication check failed:- {self.doc_name}")
    
    # def check_file_processed(self):
    #     '''
    #     ### Checks if the current document is present in the received file logs
        
    #     args:
    #         None
    #     return:
    #         status : True/False
    #         processed_doc_name : processed document name
    #         processed_doc_size : size of MR
    #     '''
    #     try:
    #         print(f"Reading received file logs to check if the document '{self.doc_name}' is processed")
    #         # Step-1 Reading received file logs
    #         response = self.s3c.get_object(Bucket = self.bucket_name, Key = self.received_file_logs_key)
    #         existing_content = response['Body'].read().decode('utf-8')

    #         # FILENAME|RECEIVED_DATE|STATUS|SIZE|ZIGNA_FILE_NAME
    #         # Step-2 Split the content into lines and process each line
    #         for line in existing_content.splitlines()[1:]:
    #             line_arr = line.split('|')
    #             if len(line_arr) >= 5:
    #                 processed_doc_name = line_arr[0]
    #                 processed_doc_size = int(line_arr[3])
    #                 # Step-3 Check if the current file name matches the processed file name
    #                 if self.doc_name.upper() == processed_doc_name.upper():
    #                     return True, processed_doc_name, processed_doc_size 
    #         #If no match is found, return False and None       
    #         return False, None, None
    #     except self.s3r.meta.client.exceptions.NoSuchKey:
    #         return False, None, None
    #     except Exception as e:
    #         print(f"An error occurred while checking if the file was processed: {e}")

    def get_cleaned_doc_name(self, doc_name):
        '''
        ### Reads the json file
        args:
            doc_name: string (document name)
        return:
            renamed_document_name: string
        '''
        pattern1 = r"([!@#$%^&*()_+={}\[\]:;\"'<>,.?/\\|`~\-])\1+"
        pattern2 = r"[^A-Za-z0-9._]+"
        doc_name_v1 = re.sub(r"[\[\]()!{}]", "", doc_name)
        doc_name_v2 = re.sub(r"\s+", " ", doc_name_v1)
        doc_name_v3 = re.sub(pattern1, r"\1", doc_name_v2)
        doc_name_v4 = doc_name_v3.strip().replace(" ", "_").replace("-","_")
        if len(re.findall(pattern2, doc_name_v4)) > 0:
            doc_name_v5 = re.sub(pattern2, "_", doc_name_v4)
            doc_name_v6 = re.sub(r"_+", r"_", doc_name_v5)
            doc_name_v6 = doc_name_v6.replace("_.pdf", ".pdf")
            print(f"Cleaned document name for '{self.doc_name}' is '{doc_name_v6}'")
            print(rf"renamed MR name for {self.doc_name}:- {doc_name_v6}")

            return doc_name_v6
        print(f"Cleaned document name for '{self.doc_name}' is '{doc_name_v4}'")
        print(rf"renamed MR name for {self.doc_name}:- {doc_name_v4}")

        return doc_name_v4

    def duplicate_check(self, client_doc_pattern_json, get_cateogery_obj, document_type, df_arl):
        '''
        ### Reads the json file
        
        args:
            client_doc_pattern_json: dict 
            get_cateogery_obj: object (Name of the S3 bucket)
        return:
            is_document_duplicated: boolean (True/False)
            document_type_code: string (cateogery)
            document_storage_path: string (s3 path)
            document_name_cleaned: string (renamed medical record name)
        '''
        format_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        doc_size = int(self.response['ResponseMetadata']['HTTPHeaders']['content-length'])
        processed_doc_status, processed_doc_name, processed_doc_size = self.check_file_processed(df_arl)
        received_date = str(parse(self.response['ResponseMetadata']['HTTPHeaders']['last-modified']))
        
        if processed_doc_status:
            # Step -4 checking if sizes are same and threshold of 0.05 MB
            if (processed_doc_size - doc_size) <= 50000:
                # if sizes are same and if it is pdf we check page count
                if self.doc_name.lower().endswith('.pdf'):
                    curr_doc_page_count = self.get_page_count("current document", self.doc_name)
                    processed_doc_page_count = self.get_page_count("processed document", processed_doc_name)

                    # Step -5, 2nd condition File '{file_name}' has the same filename, same size and same page count
                    if processed_doc_page_count == curr_doc_page_count:
                        # File is already processed/Duplicate, Move to Duplicate
                        print(f"MR '{self.doc_name}' has the same file_name, size and page numbers as '{processed_doc_name}'")
                        renamed_doc_name_ = f'{self.doc_name.lower().replace(".pdf", "").upper()}_{format_date}.pdf'
                        renamed_doc_name = self.get_cleaned_doc_name(renamed_doc_name_)
                        dest_doc_path = rf"{self.duplicate_medical_record_path}{renamed_doc_name}"
                        self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, "medical_record", doc_size, renamed_doc_name)
                        print(rf"MR is a duplicate:- {self.doc_name}")
                        return True, "medical_record", dest_doc_path, renamed_doc_name

                    # Step - 6 : 1st condition File '{file_name}' has the same filename,size but different page counts
                    else:
                        print(f"MR '{self.doc_name}' has the same file_name and size as '{processed_doc_name}' but different page numbers")
                        renamed_doc_name_ = f'{self.doc_name.lower().replace(".pdf", "").upper()}_{format_date}.pdf'
                        renamed_doc_name = self.get_cleaned_doc_name(renamed_doc_name_)
                        category, dest_doc_path = get_cateogery_obj.map_to_folder(renamed_doc_name, client_doc_pattern_json)
                        self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, renamed_doc_name)
                        print(rf"MR is not a duplicate:- {self.doc_name}")
                        return False, category, dest_doc_path, renamed_doc_name
                
                # Move to Duplicate 
                else:
                    print(f"The document '{self.doc_name}' has the same file_name and size as '{processed_doc_name}'")
                    renamed_doc_name = self.get_cleaned_doc_name(self.doc_name)
                    dest_doc_path_ = rf"{self.duplicate_document_path}{renamed_doc_name}"
                    category, dest_doc_path = get_cateogery_obj.map_to_folder(renamed_doc_name, client_doc_pattern_json)
                    self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, renamed_doc_name)
                    return True, category, dest_doc_path_, renamed_doc_name

            else:
                # print("Step -8 Sizes are different if file is pdf check for page count")
                if self.doc_name.lower().endswith('.pdf'):
                    curr_doc_page_count = self.get_page_count("current document", self.doc_name)
                    processed_doc_page_count = self.get_page_count("processed document", processed_doc_name)

                    if curr_doc_page_count == processed_doc_page_count:
                        # print(f"Step -9 3rd condition file '{self.doc_name}' has the same filename and pagecount but size is different ")
                        print(f"The MR '{self.doc_name}' has the same file_name and page numbers as '{processed_doc_name}' but different size")
                        # Move to Manual Review, Move with Zigna renamed file name, rename Docuemnt update the received filelogs, if necessary process it manually
                        renamed_doc_name_ = f'{self.doc_name.lower().replace(".pdf", "").upper()}_{format_date}.pdf'
                        renamed_doc_name = self.get_cleaned_doc_name(renamed_doc_name_)
                        dest_doc_path = rf"{self.medical_record_manual_review_path}{renamed_doc_name}"
                        self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, "medical_record", doc_size, renamed_doc_name)
                        print(rf"MR is moved to manual review suspected duplicate:- {renamed_doc_name}")
                        return True, "medical_record" ,dest_doc_path, renamed_doc_name
                    else:
                        # print(f"Step -10, 1st condition File '{self.doc_name}' has different sizes and different page counts")
                        print(f"The MR '{self.doc_name}' has the same file_name as '{processed_doc_name}' but different page numbers and sizes")
                        renamed_doc_name_ = f'{self.doc_name.lower().replace(".pdf", "").upper()}_{format_date}.pdf'
                        renamed_doc_name = self.get_cleaned_doc_name(renamed_doc_name_)
                        category, dest_doc_path = get_cateogery_obj.map_to_folder(renamed_doc_name, client_doc_pattern_json)
                        self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, renamed_doc_name)
                        return False, category, dest_doc_path, renamed_doc_name
                else:
                    print(f"The document '{self.doc_name}' has same file name as '{processed_doc_name}' but different size")
                    # Move to Manual Review
                    renamed_doc_name = self.get_cleaned_doc_name(self.doc_name)
                    dest_doc_path_= rf"{self.document_manual_review_path}{renamed_doc_name}"
                    category, dest_doc_path = get_cateogery_obj.map_to_folder(renamed_doc_name, client_doc_pattern_json)
                    self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, renamed_doc_name)
                    return False, category, dest_doc_path_, renamed_doc_name
        else:
            #Different file name and size
            if document_type.lower() == 'pdf':
                print(f"The document '{self.doc_name}' is unique")
                renamed_doc_name_ = f'{self.doc_name.lower().replace(".pdf", "").upper()}_{format_date}.pdf'
                renamed_doc_name = self.get_cleaned_doc_name(renamed_doc_name_)
                print("Renamed MR name : ", renamed_doc_name)
                category, dest_doc_path = get_cateogery_obj.map_to_folder(renamed_doc_name, client_doc_pattern_json)
                self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, renamed_doc_name)
                print(rf"MR is not a duplicate:- {renamed_doc_name}")

                return False, category, dest_doc_path, renamed_doc_name
            else:
                print(f"The document '{self.doc_name}' is unique")
                category, dest_doc_path = get_cateogery_obj.map_to_folder(self.doc_name, client_doc_pattern_json)
                self.received_file_logs_obj.update_received_file_logs(self.doc_name, received_date, category, doc_size, self.doc_name)
                return False, category, dest_doc_path, self.doc_name
        