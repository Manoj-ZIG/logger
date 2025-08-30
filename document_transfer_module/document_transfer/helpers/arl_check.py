import re
import pandas as pd
from datetime import datetime
from helpers.constant import *
from dateutil.parser import parse

try:
    from datetime_module.datetime_extractore import DatetimeExtractor
    from helpers.generate_validation_query import generate_query
except ModuleNotFoundError as e:
    from ..datetime_module.datetime_extractore import DatetimeExtractor
    from ..helpers.generate_validation_query import generate_query

class UniqueClaimIdentifierCheck:
    def __init__(self, s3c, athena_client, client_name,document_name_cleaned, client_doc_pattern_json, date_tag_constants, adjudication_record_locator, root_payer_control_number):
        self.s3c = s3c
        self.athena_client = athena_client
        self.client_name = client_name
        self.document_name_cleaned=document_name_cleaned
        self.client_doc_pattern_json = client_doc_pattern_json
        self.manual_review = f"{client_name}/audits/sftp/medical_records/manual_review/unique_claim_identifier/"
        self.medical_record_json = [json for json in self.client_doc_pattern_json['get_document_info'] if json['document_name'] == 'medical_record'][0]
        self.date_tag_constants = date_tag_constants
        self.adjudication_record_locator = adjudication_record_locator
        self.root_payer_control_number = root_payer_control_number

    def get_unique_claim_identifier_dictionary(self, data):
        '''
        ### Returns the unique_claim_identifiers found in the given json data 
        
        args:
            s3c: s3 client
            bucket: string (Name of the S3 bucket)
            json_file_path: string (path of the file)
        return:
            count_dict: dictionary
        '''
        if not data:
            print(f"json data is empty")
            return {}
        count_dict = {}
        pattern = self.medical_record_json['pattern']
        for item in data:
            blocks = item.get('Blocks',[])
            for block in blocks:
                blocktype = block.get('BlockType','')
                page_num = block.get('Page',-1)
                text = block.get('Text',"")
                if blocktype=="LINE" and 1<=page_num<=10:
                    matches = re.findall(pattern,text)
                    for match in matches:
                        count_dict[match] = count_dict.get(match,0) + 1
                if page_num>10:
                    break
        return count_dict
    
    def parse_dates(self, dates):
        '''
        ### Returns the the list of dates by parsing them
        
        args:
            dates: list (list of dates) 
        return:
            parsed_dates: list (list of dates)
        '''
        parsed_dates = []
        for date in dates:
            try:
                parsed_dates.append(parse(date).date())
            except Exception as e:
                print(e)
        return parsed_dates
    
    def normalize_json(self, json_data):
        '''
        ### Reads the json and converts it into a dataframe
        
        args:
            json_data: json
        return:
            df: dataframe
        '''
        df1 = pd.DataFrame()
        for x in json_data:
            df = pd.json_normalize(x['Blocks'])
            df1 = pd.concat([df1, df])    
        return df1
    
    def get_corpus(self, df):
        """
        ### Get the corpus of the entire DataFrame
        args:
            df: DataFrame
        return:
            corpus: Text
        """
        corpus = ' '.join(df['Text'].apply(lambda x: str(x)))
        return corpus
    
    def validate_phi(self, phi_df, text):
        '''
        ### Returns the True/False by validating the member_first_name, member_last_name and dob
        
        args:
            phi_df: dataframe 
            text: string (text)
        return:
            is_phi_present: bool (True/False)
        '''
        print(f'checking if patient DOB and Name matching:- {self.document_name_cleaned}')
        is_member_dob_match = False
        is_member_name_match = False
        dob_flag = False
        if not phi_df.empty:
            member_first_name = phi_df.iloc[0]['member_first_name']
            member_last_name = phi_df.iloc[0]['member_last_name']
            member_dob = phi_df.iloc[0]['member_dob']
            member_dob = str(parse(member_dob))[:10]

            member_name = member_first_name.lower() + " " + member_last_name.lower()
            member_name = re.sub(r"\s+", " ", member_name)
            member_name_ls = member_name.split(" ")
            sub_name_count = 0
            for sub_name in member_name_ls:
                if sub_name in text.lower():
                    sub_name_count += 1
            
            if sub_name_count == len(member_name_ls):
                is_member_name_match = True
                print(f"The patient name is matching for the ARL '{phi_df.iloc[0]['adjudication_record_locator']}'")

            date_str_obj_ls = DatetimeExtractor.get_date_time_from_corpus_v2(text.lower(), self.date_tag_constants['date_of_birth_tags'])
            
            # Step1 : Considering that the dob tags are present in the detected dates [(date, time, tag)]
            extractedDOBs = [i[0] for i in date_str_obj_ls if i[2] in self.date_tag_constants['date_of_birth_tags']]
            extractedDOBs = list(set(extractedDOBs))
            parsed_extractedDOBs = self.parse_dates(extractedDOBs)
            sorted_extractedDOBs = sorted(parsed_extractedDOBs)

            sorted_extractedDOBs = [str(i) for i in sorted_extractedDOBs]
            extractedDOB = sorted_extractedDOBs[0]

            if extractedDOB == member_dob:
                is_member_dob_match = True
                dob_flag = True
                print(f"The patient dob is matching for the ARL '{phi_df.iloc[0]['adjudication_record_locator']}'")
            
            # Step2 : Considering that the dob tags are not present in the detected dates [(date, time, tag)], we sort all the detected dates.
            if not dob_flag:
                extracted_dates = [i[0] for i in date_str_obj_ls if i[0] != re.findall(r"(\d{,4})", i[0])[0]]
                extracted_dates = list(set(extracted_dates))
                parsed_extracted_dates = self.parse_dates(extracted_dates)
                sorted_extracted_dates = sorted(parsed_extracted_dates)
                sorted_extracted_dates = [str(i) for i in sorted_extracted_dates]
                extractedDOB = sorted_extracted_dates[0]
                if extractedDOB == member_dob:
                    is_member_dob_match = True
                    dob_flag = True
                    print(f"The patient dob is matching for the ARL '{phi_df.iloc[0]['adjudication_record_locator']}'")
        if is_member_dob_match==False:
            print(f'patient DOB not matched:- {self.document_name_cleaned}')
        if is_member_name_match==False:
            print(f'patient Name not matched:- {self.document_name_cleaned}')

        return is_member_name_match, is_member_dob_match
            
    def get_unique_claim_identifier_status(self, bucket_name, key, json_file_path):
        '''
        ### Returns the unique_claim_identifiers found in the given json data 
        
        args:
            bucket: string (Name of the S3 bucket)
            key: string (path of the file (MR))
            json_file_path: string (path of the file(json path))
        return:
            dest_key: dictionary
            is_unique_claim_identifier_present_in_document_name: 0/1 (0 - Not present, 1 - Present)
            is_unique_claim_identifier_received: 0/1 (0 - Not received, 1 - Received)
            is_unique_claim_identifier_matched_with_document: 0/1 (0 - Not matched, 1 - Matched)
            is_document_sent_for_manual_review: 0/1 (0 - Not Sent for manual review, 1 - Sent for Manual Review)
        '''
        data = read_json_from_s3(self.s3c, bucket_name, json_file_path)
        df = self.normalize_json(data)
        df = df[df['BlockType'] == 'LINE'].reset_index(drop=True)
        corpus = self.get_corpus(df)
        print(f'validation query starting:- {self.document_name_cleaned}')
        validation_query_df = generate_query(self.athena_client, self.s3c, self.root_payer_control_number, self.client_doc_pattern_json)
        print(f'validation query completed:- {self.document_name_cleaned}')

        is_member_name_match, is_member_dob_match = self.validate_phi(validation_query_df, corpus)
        
        print(f'Collecting all possible ARLs from PDF:- {self.document_name_cleaned}')
        count_dict = self.get_unique_claim_identifier_dictionary(data)
        print("count dict : ",count_dict)
        count_list = list(sorted(count_dict.items(), key= lambda x:-x[1]))
        print(f"count list : {count_list}")
        updated_pdf_name = ''
        is_unique_claim_identifier_received = 0
        is_unique_claim_identifier_matched_with_document = 0
        is_document_sent_for_manual_review = 0
        pdf_name = key.split('/')[-1]
        #unique_claim_identifier = pdf_name.split("_")[0]
        pattern = self.medical_record_json['pattern']
        unique_claim_identifier_list = re.findall(pattern,pdf_name)
        if len(unique_claim_identifier_list)>0:
            unique_claim_identifier = unique_claim_identifier_list[0].upper()
        else:
            unique_claim_identifier = ''

        prefix_s3_name = '/'.join(key.split('/')[:-1])
        if len(unique_claim_identifier)>0:
            is_unique_claim_identifier_present_in_document_name = 1
            if unique_claim_identifier in count_dict.keys() and is_member_dob_match and is_member_name_match:
                dest_key = key
                is_unique_claim_identifier_received = 1
                is_unique_claim_identifier_matched_with_document = 1
                is_document_sent_for_manual_review = 0
            else:
                is_unique_claim_identifier_matched_with_document = 0
                is_document_sent_for_manual_review = 1
                if len(count_list)==0:
                    dest_key = self.manual_review + pdf_name
                    # copy_object(self.s3c, bucket_name, key, bucket_name, dest_key)
                    # delete_object(self.s3c, bucket_name, key)
                else:
                    unique_claim_identifier_new = count_list[0][0]
                    #rename pdf with unique_claim_identifier
                    is_unique_claim_identifier_received = 1
                    updated_pdf_name = pdf_name
                    #dest_key = prefix_s3_name + "/"+ updated_pdf_name
                    dest_key = self.manual_review + updated_pdf_name
                    # copy_object(self.s3c, bucket_name, key, bucket_name, dest_key)
                    # delete_object(self.s3c, bucket_name, key)
        else:
            is_unique_claim_identifier_present_in_document_name = 0
            is_document_sent_for_manual_review = 1
            if len(count_list)==0:
                is_unique_claim_identifier_received = 0
                is_unique_claim_identifier_matched_with_document =  0
                dest_key = self.manual_review + pdf_name
                # copy_object(self.s3c, bucket_name, key, bucket_name, dest_key)
                # delete_object(self.s3c, bucket_name, key)
            else:
                is_unique_claim_identifier_received = 1
                is_unique_claim_identifier_matched_with_document =  0
                unique_claim_identifier = count_list[0][0]
                #rename pdf with unique_claim_identifier
                updated_pdf_name = unique_claim_identifier+"_"+pdf_name
                #dest_key = prefix_s3_name +"/" + updated_pdf_name
                dest_key = self.manual_review + updated_pdf_name
                # copy_object(self.s3c, bucket_name, key, bucket_name, dest_key)
                # delete_object(self.s3c,  bucket_name, key)
        if is_document_sent_for_manual_review == 1:
            print(f"File went to manual review because of PHI mismatch:- {self.document_name_cleaned}")
        return dest_key ,is_unique_claim_identifier_present_in_document_name,is_unique_claim_identifier_received,is_unique_claim_identifier_matched_with_document, is_document_sent_for_manual_review, validation_query_df