import re
from datetime import datetime
try:
    from helpers.constant import copy_object, delete_object
except ModuleNotFoundError as e:
    from ..helpers.constant import copy_object, delete_object

class GetCateogery:
    def __init__(self, s3c, s3r, bucket_name, key, client_name, document_name, common_prefix):
        self.s3c = s3c
        self.s3r = s3r
        self.key = key
        self.doc_name = document_name
        self.client_name = client_name
        self.bucket_name = bucket_name
        self.common_prefix = common_prefix

    def extract_filename(self, key):
        print("Step -1 Extracts and cleans the filename from the given key")
        try:
            print("Step -2 Split the key to get the filename (assuming '|' is the delimiter")
            # file_name = key.split("|")[0]
            file_name = key.split("/")[-1]
            file_name_upper = file_name.upper()
            file_name_clean = re.sub(r"[^A-Za-z0-9]", "", file_name_upper)
            return file_name_clean.strip()
        except Exception as e:
            print(f"Error extracting filename: {e}")
            return ""

    def map_to_folder(self, doc_name, client_doc_pattern_json):
        '''
        ### Copies the s3 file to another location
        
        args:
            doc_name: string (document name)
            client_doc_pattern_json: dict (json dict)
        return:
            document_type_code: string (cateogery)
            document_storage_path_: string (destination document key)
        '''
        format_date = datetime.now().strftime("%Y%m%d")

        file_name_upper = doc_name.upper()
        file_name_upper = re.sub(r"[^A-Za-z0-9]","",file_name_upper)
        output_ls = [(re.findall(f"{json['pattern']}", rf"{file_name_upper}"), json) for json in client_doc_pattern_json['get_document_info']]
        if any(sublist for sublist in output_ls if sublist[0]):
            document_info = next((sublist for sublist in output_ls if sublist[0]), None)
            if document_info[1]['document_name'] == 'raw_claims':
                sub_folder_name =  re.findall(document_info[1]['pattern'],file_name_upper)[0]
                return document_info[1]['document_name'], self.common_prefix + document_info[1]['output_folder_path'] + sub_folder_name + "/" + doc_name
            if document_info[1]['date_flag'] == 1:
                return document_info[1]['document_name'], self.common_prefix + document_info[1]['output_folder_path'] + format_date + "/" + doc_name
            return document_info[1]['document_name'], self.common_prefix + document_info[1]['output_folder_path'] + doc_name
        return "unknown_category", self.common_prefix + "manual_review/" + doc_name
