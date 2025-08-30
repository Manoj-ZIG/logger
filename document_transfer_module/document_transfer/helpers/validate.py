from io import BytesIO,BufferedReader
import pandas as pd
import PyPDF2
import random


def run_query(athena_client, s3_client, query_string, source_db, QUERY_OUTPUT_LOCATION, WORKGROUP):
    
    """
    Execute query for athena client and save result at S3 path location using boto3.

    args: 
        client: aws athena client service in boto3
        query_string: query to run in athena 
        source_db: Database related to aws glue data catalog on which query is executed
        OutputLocation: S3 bucket location at which result is saved in csv

    return:
        df: output result as pandas dataframe object
    """
    
    try:
        response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': source_db
            },
            ResultConfiguration={
                'OutputLocation': QUERY_OUTPUT_LOCATION
            },
            # WorkGroup=WORKGROUP
        )
        query_execution_id = response['QueryExecutionId']
        
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id) 
            query_state = query_status['QueryExecution']['Status']['State'] 
            query_result_path = query_status['QueryExecution']['ResultConfiguration']['OutputLocation']  
            if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

        if query_state == 'SUCCEEDED':
            print("QUERY EXECUTION SUCCESSFUL: {}".format(query_state))
            try:
                bucket = query_result_path.split('//')[1].split('/')[0]
                key = '/'.join(query_result_path.split('//')[1].split('/')[1:])
                buffer = BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()) 
                df = pd.read_csv(buffer, dtype=str) 
                return df
            except Exception as e:
                print("QUERY EXECUTION SUCCESSFULL BUT GOT ERROR WHILE CONVERSION TO DATAFRAME")
                print(e)
        else:
            print("QUERY EXECUTION FAILED: {}".format(query_state))
            return None
    except Exception as e:
            print("GOT EXCEPTION WHILE RUNNING QUERY")
            print(e)
            return None


def check_if_post_pay_claim_exists(database_name,claims_transformed_table,comparision_column_list,comparision_column,unique_claim_id):
     
    claim_exist_check_query_string = None
    try:
        claim_exist_check_query_string = f'''select distinct {", ".join(comparision_column_list)} 
                                                from "{database_name}".{claims_transformed_table} 
                                                where root_payer_control_number (  
                                                        select distinct root_payer_control_number
                                                            from "{database_name}".{claims_transformed_table}
                                                            where {comparision_column} = '{unique_claim_id}' ) '''
    except Exception as e:
        print(f'Error generating claim validation query - {e}')
    return claim_exist_check_query_string


def check_if_pre_pay_claim_exists(database_name,prepay_claims_transformed_table,comparision_column_list,comparision_column,unique_claim_id):
     
    claim_exist_check_query_string = None
    try:
        claim_exist_check_query_string = f'''select distinct {", ".join(comparision_column_list)} 
                                                            from "{database_name}".{prepay_claims_transformed_table}
                                                            where {comparision_column} = '{unique_claim_id}' ) '''
    except Exception as e:
        print(f'Error generating claim validation query - {e}')
    return claim_exist_check_query_string


def generate_claim_validate_query_string(database_name, selections_table, claim_submission_table, submission_response_table, 
                                        unique_claim_id_tuple,claim_validate_select_list):
	claim_validate_query_string = None
	try:
		claim_validate_query_string = f'''SELECT distinct {", ".join(claim_validate_select_list)}
											FROM "{database_name}"."{selections_table}"
												left join "{database_name}"."{claim_submission_table}" on {claim_submission_table}."adjudication record locator" = {selections_table}.adjudication_record_locator
												left join "{database_name}"."{submission_response_table}" on {submission_response_table}."vendor audit id" = {selections_table}.vendor_audit_id
											where adjudication_record_locator in {unique_claim_id_tuple}'''
	except Exception as e:
		print(f'Error generating claim validation query - {e}')
	return claim_validate_query_string


def generate_lifecycle_exist_query_string(database_name, lifecycle_table, adjudication_record_locator):
	lifecycle_exist_query_string = None
	try:
		lifecycle_exist_query_string = f'''SELECT distinct adjudication_record_locator, lifecycle_sent_date
											FROM "{database_name}"."{lifecycle_table}"
											where adjudication_record_locator in {adjudication_record_locator}'''
	except Exception as e:
		print(f'Error generating claim validation query - {e}')
	return lifecycle_exist_query_string



def check_page_count_digitize_corrupt_status(pdf_file,max_page_threshold):

    page_count_digitize_corrupt_status_dict = {'is_digitized':0, 'page_count':-1,'is_corrupted':0}

    try:
        with pdf_file as local_pdf:
            pdf_reader = PyPDF2.PdfReader(local_pdf)
            num_pages = len(pdf_reader.pages)
            try:
                extracted_text = ''
                if num_pages != -1:
                    page_list = {random.randint(0,num_pages) for r in range(max_page_threshold)}
                    for page_num in page_list:
                        page = pdf_reader.pages[page_num]
                        extracted_text = page.extract_text()
                        extracted_text += ''
                    if extracted_text:
                        is_digitised = 1
                        page_count_digitize_corrupt_status_dict.update({'is_digitized':is_digitised, 'page_count':num_pages,'is_corrupted':is_corrupted})
                        return page_count_digitize_corrupt_status_dict
                else:
                    print(f"Unable to collect pages from pdf {pdf_file}")
                    is_digitised = 0
                    num_pages = -1
                    page_count_digitize_corrupt_status_dict.update({'is_digitized':is_digitised, 'page_count':num_pages,'is_corrupted':is_corrupted})
                    return page_count_digitize_corrupt_status_dict                
            except:
                is_digitised = 0
                page_count_digitize_corrupt_status_dict.update({'is_digitized':is_digitised, 'page_count':num_pages,'is_corrupted':is_corrupted})
                return page_count_digitize_corrupt_status_dict
    except:
        print(f"Unable to open pdf {pdf_file}")
        is_corrupted = 1
        page_count_digitize_corrupt_status_dict.update({'is_digitized':is_digitised, 'page_count':num_pages,'is_corrupted':is_corrupted})
        return page_count_digitize_corrupt_status_dict