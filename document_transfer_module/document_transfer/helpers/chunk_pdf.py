import PyPDF2
import traceback
from datetime import datetime
from io import BytesIO,BufferedReader

try:
    from helpers.constant import copy_object, delete_object
except ModuleNotFoundError as e:
    from ..helpers.constant import copy_object, delete_object

class PDF:
    def __init__(self, s3c, s3r,document_name_cleaned, MEDICAL_RECORD_PATH, client_name, batch_size = 550):
        self.s3c = s3c
        self.s3r = s3r
        self.document_name_cleaned=document_name_cleaned
        self.MEDICAL_RECORD_PATH = MEDICAL_RECORD_PATH
        self.client_name = client_name
        self.batch_size = batch_size
        self.MAX_PAGE_THRESHOLD = 50


    def digitise_check(self, pdf_file):
        '''
        ### This function checks if the pdf is digitised
        
        args:
            pdf_file: pdf document
        return:
            is_digitised: int (1/0)
            is_corrupted: int (1/0)
            num_pages: int (number of pages)
        '''
        is_digitised = 0
        num_pages = -1
        is_corrupted = 0
        try:
            with pdf_file as local_pdf:
                pdf_reader = PyPDF2.PdfReader(local_pdf)
                num_pages = len(pdf_reader.pages)
                try:
                    extracted_text = ''
                    print(f"digitise_check for {pdf_file} - number of pages : {min(num_pages,self.MAX_PAGE_THRESHOLD)}")
                    for page_num in range(0,min(num_pages,self.MAX_PAGE_THRESHOLD)):
                        # print('page_num',page_num)
                        page = pdf_reader.pages[page_num]
                        extracted_text = page.extract_text()
                        if extracted_text:
                            is_digitised = 1
                            break
                except:
                    print(traceback.format_exc())
                    is_digitised = 0
        except:
            print(f"Unable to open pdf {pdf_file}")
            print(traceback.format_exc())
            is_corrupted = 1
            print(f"digitization check failed:- {self.document_name_cleaned}")
            print(f"corrupted pdf:- {self.document_name_cleaned}")
            return is_digitised , is_corrupted , num_pages
        return is_digitised ,is_corrupted , num_pages

    def get_output_part_name(self, key, part):
        '''
        ### This function returns the zigna file name
        
        args:
            key: s3 key
            part: int (part number)
        return:
            key: zigna file name
        '''
        return key.replace('.pdf','')+f'_ZIG_MR_{part}.pdf'

    def chunk_raw_pdf(self, input_bucket, input_key, output_bucket, output_key):
        '''
        ### Reads the pdf file and chunks it into 550 page
        
        args:
            batch_size: int (the max page a pdf can have)
        return:
            chunk_paths: chunked pdf paths
        '''
        
        chunk_paths = []    
        response = self.s3c.get_object(Bucket=input_bucket, Key=input_key)
        fs = response['Body'].read()
        size = response['ContentLength']
        try:
            pdfFile = PyPDF2.PdfReader(BufferedReader(BytesIO(fs)))
            no_of_pages = len(pdfFile.pages)
            print(f"Total number of pages in {input_key} - {no_of_pages}")
            print(f'No of pages: {no_of_pages}:- {self.document_name_cleaned}')
            if no_of_pages <= self.batch_size:
                output_key = output_key.replace(".pdf", f"_part_0_{no_of_pages}.pdf")
                print("Generated a PDF chunk : ", output_key)
                copy_object(self.s3c, input_bucket, input_key, output_bucket, output_key)
                chunk_paths.append((output_key, no_of_pages))
                return chunk_paths
            total_parts = (no_of_pages+self.batch_size-1)//self.batch_size  
            min_pages_for_chunk = no_of_pages//total_parts
            chunks = [min_pages_for_chunk]*total_parts
            extra_pages = no_of_pages%total_parts
            for index in range(extra_pages):
                chunks[index] += 1
            part = 1
            pdfwriter = PyPDF2.PdfWriter()
            curr_max_pages = chunks[0]
            for index in range(no_of_pages):
                page = pdfFile.pages[index]
                pdfwriter.add_page(page)
                if (index+1) == curr_max_pages:
                    page_count = chunks[part -1]
                    with BytesIO() as bytes_stream:
                        pdfwriter.write(bytes_stream)
                        output_file_name = output_key.replace('.pdf','')+f'_ZIG_MR_{part}_part_{part}_{no_of_pages}.pdf'
                        chunk_paths.append((output_file_name,page_count))
                        self.s3r.Object(output_bucket,output_file_name).put(Body=bytes_stream.getvalue(),ContentType = 'application/pdf')
                        print(f"Moved pdf to raw folder QA:- {output_file_name.split('/')[-1]}")
                        print("Generated a PDF chunk : ", output_file_name)
                    if part!=total_parts:
                        curr_max_pages += chunks[part]
                        part+=1
                        pdfwriter = PyPDF2.PdfWriter()
        except Exception as e:
            print(f"Error in extracting text from textract : {e}")
        return chunk_paths

    def pdf_manifest(self, input_bucket, input_key, document_storage_path, validation_query_df, vendor_code, adjudication_record_locator):
        output_bucket = input_bucket
        manifest_files = []
        is_zigna_manifest = 1
        old_pdf_name = input_key.split('/')[-1]
        pdf_name = document_storage_path.split('/')[-1]
        pdf_adj = adjudication_record_locator
        folder_name_1 = pdf_name.replace(".pdf","")+"/"
        
        existing_content = f"Vendor Code|Vendor Audit ID|Client Audit ID|Adjudication Record Locator|Member Record Locator|Document Type Code|PDF_Name|Document Rec Date|Action Date|zigna_s3_path|zip_file_name\n"
        
        format_date = datetime.now().strftime("%Y%m%d")
        folder_name_date = f'{str(datetime.now().strftime("%Y%m%d%H%M%S"))}'

        zigna_s3_path =  self.MEDICAL_RECORD_PATH + 'processed'+ '/' + format_date+'/'+'manifest/'+ document_storage_path.split('/')[-1].replace(".pdf","")
        try:
            vendor_code = vendor_code if validation_query_df['vendor code'].iloc[0] == '' else validation_query_df.iloc[0]['vendor code']
        except:
            vendor_code = ''
        try:
            vendor_audit_id = validation_query_df.iloc[0]['vendor audit id']
        except:
            vendor_audit_id = ''
        pdf_name_map = dict()
        pdf_name_map[old_pdf_name] = pdf_name
        submission_response_adj_list = validation_query_df['adjudication_record_locator'].to_list() if not validation_query_df.empty else []
        existing_content   += f"{vendor_code}|{vendor_audit_id}||{str(pdf_adj)}||Medical Records|{pdf_name}|{format_date}|{format_date}|{zigna_s3_path}|{old_pdf_name}\n"

        manifest_path = f"{zigna_s3_path}.txt"
        self.s3c.put_object(Bucket=output_bucket,Key=manifest_path,Body=existing_content.encode('utf=8'))
        manifest_files.append(manifest_path)

        try:
            folder_name = self.MEDICAL_RECORD_PATH + 'processed' + '/' +format_date+ '/'+'medical_records/'+document_storage_path.split('/')[-1]
            print(f'copying pdf to processed folder:- {self.document_name_cleaned}')
            copy_object(self.s3c, input_bucket, document_storage_path, output_bucket, folder_name)
            print(f'copied pdf to processed folder:- {self.document_name_cleaned}')

            return  1, manifest_files, pdf_name, is_zigna_manifest, pdf_name_map
        except Exception as e:
            error_zip_folder =  'error'+ '/' + format_date+'/'
            copy_object(self.s3c, input_bucket, input_key, output_bucket, self.MEDICAL_RECORD_PATH + error_zip_folder + input_key.split("/")[-1])
            print(f'copying pdf to error folder:- {self.document_name_cleaned}')

            # print(f"EC2 copying from {input_key} to {input_key.replace('input_files', error_zip_folder)}")
            # delete_object(bucket_name, key)
            is_arl_present,is_arl_received,is_arl_matched  = -1,-1,-1
            return  0, [], '', is_zigna_manifest, pdf_name_map