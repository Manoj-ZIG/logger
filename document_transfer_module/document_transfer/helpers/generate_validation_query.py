import pandas as pd
try:
    from helpers.validate import run_query
    from helpers.constant import workgroup, query_output_location
except ModuleNotFoundError as e:
    from ..helpers.validate import run_query
    from ..helpers.constant import workgroup, query_output_location

def generate_query(athena_client, s3c, root_payer_control_number, client_doc_pattern_json):
    source_db = f"{client_doc_pattern_json.get('client_name')}_prod"
    table_source_name1 = "transformed_claims"
    table_source_name2 = "claim_submissions"
    table_source_name3 = "submission_response"
    table_source_name4 = "selections"

    query_string = f"""SELECT distinct {table_source_name1}.root_payer_control_number,
                    {table_source_name1}.payer_control_number,
                    {table_source_name1}.adjudication_record_locator,
                    {table_source_name1}.claim_id,
                    {table_source_name1}.bill_type_code,
                    {table_source_name1}.billed_drg,
                    {table_source_name1}.priced_drg,
                    {table_source_name1}.admission_date,
                    {table_source_name1}.discharge_date,
                    {table_source_name1}.length_of_stay,
                    {table_source_name1}.billing_provider_npi,
                    {table_source_name1}.billing_provider_tin,
                    {table_source_name1}.attending_provider_npi,
                    {table_source_name1}.attending_provider_name,
                    {table_source_name1}.member_first_name,
                    {table_source_name1}.member_last_name,
                    {table_source_name1}.member_dob,
                    {table_source_name4}.query_name,
                    {table_source_name4}.template_name,
                    {table_source_name4}.priority,
                    {table_source_name4}.submission_date,
                    {table_source_name2}."vendor code",
                    {table_source_name2}."vendor audit id"
                FROM {table_source_name2}
                    left join {table_source_name1} on {table_source_name2}."adjudication record locator" = {table_source_name1}.adjudication_record_locator
                    left join {table_source_name4} on {table_source_name2}."adjudication record locator" = {table_source_name4}."adjudication_record_locator"
                    left join {table_source_name3} on {table_source_name3}."vendor audit id" = {table_source_name2}."vendor audit id"
                where {table_source_name3}.status like '%APPROVED%' and {table_source_name1}.root_payer_control_number in ('{root_payer_control_number}')
                order by submission_date asc"""
    
    df = run_query(athena_client, s3c, query_string, source_db, query_output_location, workgroup)
    try:
        print("Shape of the validation_query_df : ", df.shape)
    except:
        df = pd.DataFrame({})
    return df

def get_all_pcn_and_arl(athena_client, s3c, adjudication_record_locator, client_doc_pattern_json):
    source_db = f"{client_doc_pattern_json.get('client_name')}_prod"
    table_source_name1 = "transformed_claims"
    query_string = f"""select distinct {table_source_name1}.root_payer_control_number,
                        {table_source_name1}.payer_control_number,
                        {table_source_name1}.adjudication_record_locator
                    from {table_source_name1}
                    where root_payer_control_number in (
                            select distinct {table_source_name1}.root_payer_control_number
                            from {table_source_name1}
                            where {table_source_name1}.adjudication_record_locator in ('{adjudication_record_locator}'))"""
    df = run_query(athena_client, s3c, query_string, source_db, query_output_location, workgroup)
    try:
        print("Shape of the query(getting all the versions of the ARL with RPCN) : ", df.shape)
    except:
        df = pd.DataFrame({})
    return df