import pandas as pd
import io
from email_body import send_email_via_ses
from constants import DEST_FOLDER_LEVEL_1,DEST_FOLDER_LEVEL_2,DEST_FOLDER_LEVEL_4,DEST_FOLDER_LEVEL_5,DEST_FOLDER_LEVEL_6,DEST_FOLDER_LEVEL_7


def check_updated_policy(response1, PAYOR_NAME_DEST_FOLDER_LEVEL_3,sub_type,df,download_date,bucket_name,s3_client, filename):

    latest_run_df = df 
    previous_run_df = pd.read_csv(response1['Body'])
    print(latest_run_df.shape)
    print(previous_run_df.shape)
    #change later
    latest_run_df.rename(columns={'pdf_links':'extrctn_pdf_links','status':'extrctn_status'},inplace=True)
    previous_run_df.rename(columns={'pdf_links':'extrctn_pdf_links','status':'extrctn_status'},inplace=True)

    latest_run_df.dropna(axis = 0, how = 'all', inplace = True)
    previous_run_df.dropna(axis = 0, how = 'all', inplace = True)

    previous_run_df = previous_run_df[~(previous_run_df['extrctn_status']=='removed')]
    
    previous_run_df['extrctn_status']='no change'
    total_policies_latest_run = len(set(latest_run_df['extrctn_pdf_links']))
    total_policies_previous_run = len(set(previous_run_df['extrctn_pdf_links']))
    common_policies_latest_previous_run = len(set(previous_run_df['extrctn_pdf_links']).intersection(set(latest_run_df['extrctn_pdf_links'])))
    removed_policies_latest_run_from_previous_run = len(set(previous_run_df['extrctn_pdf_links']).difference(set(latest_run_df['extrctn_pdf_links'])))
    new_policies_in_latest_run_not_in_previous_run = len(set(latest_run_df['extrctn_pdf_links']).difference(set(previous_run_df['extrctn_pdf_links'])))

    print(f"Total {sub_type} policy links in latest run {total_policies_latest_run}")
    print(f"Total {sub_type} policy links in previous run {total_policies_previous_run}")
    print(f"Compared to previous and latest run based on the policy url,")
    print(f"      Common {sub_type} payment policies links {common_policies_latest_previous_run}")
    print(f"      Removed {sub_type} payment policies links {removed_policies_latest_run_from_previous_run}")
    print(f"      New {sub_type} payment policies links {new_policies_in_latest_run_not_in_previous_run}")

    html_body = f"""
    <html>
    <body>
       <p>Hi Team,</p>
       <p> Hope this email finds you well.</p>
       <h2><font color="purple">{sub_type.upper()} Payment Policies Extraction Summary</font></h2>
        <p>Total {sub_type} base urls used to extracted the policies <strong>{len(set(latest_run_df['extrctn_url']))}</strong></p>
        <p>Total {sub_type} policy links in latest run {total_policies_latest_run}</p>
        <p>Total {sub_type} policy links in previous run {total_policies_previous_run}</p>
        <p>Compared to previous and latest run based on the policy url,</p>
        <p>Common {sub_type} payment policies links {common_policies_latest_previous_run}</p>
        <p>Removed {sub_type} payment policies links {removed_policies_latest_run_from_previous_run}</p>
        <p>New {sub_type} payment policies links {new_policies_in_latest_run_not_in_previous_run}</p>
        <br>

        <p> To download the latest {sub_type} payment policies, please click <a href="https://{bucket_name}.s3.amazonaws.com/{filename}">here</a></p>
        <p> For more queries, please reply to the sender of this email.</p>
        <p>Regards</p>
        <p>RevMaxAI - ZignaAI</p>
    </body>
    </html>
    """

    new_policies_df = pd.DataFrame()
    removed_policy_df = pd.DataFrame()

    if len(list(set(latest_run_df['extrctn_pdf_links']).difference(set(previous_run_df['extrctn_pdf_links'])))) > 0:
        new_policies_df = latest_run_df[latest_run_df['extrctn_pdf_links'].isin(list(set(latest_run_df['extrctn_pdf_links']).difference(set(previous_run_df['extrctn_pdf_links']))))]
        # new_policies_df = new_policies_df.reset_index(drop=True)
        new_policies_df['extrctn_status'] = 'new'
        # print(len(new_policies_df))
        print(new_policies_df.shape)

    if len(list(set(previous_run_df['extrctn_pdf_links']).difference(set(latest_run_df['extrctn_pdf_links'])))) > 0:
        removed_policy_df = previous_run_df[previous_run_df['extrctn_pdf_links'].isin(list(set(previous_run_df['extrctn_pdf_links']).difference(set(latest_run_df['extrctn_pdf_links']))))]
        # removed_policy_df = removed_policy_df.reset_index(drop=True)
        removed_policy_df['extrctn_status'] = 'removed'
        # print(len(removed_policy_df))
        print(removed_policy_df.shape)

    if len(new_policies_df) > 0:
        rem_policies_df = latest_run_df[~latest_run_df['extrctn_pdf_links'].isin(list(set(new_policies_df['extrctn_pdf_links'])))]
        # rem_policies_df = rem_policies_df.reset_index(drop=True)
        rem_policies_df['extrctn_status'] = 'no change'
        print(rem_policies_df.shape)
    else:
        rem_policies_df = latest_run_df
        print('else ', rem_policies_df.shape)
    all_policies_df = pd.concat([new_policies_df,removed_policy_df,rem_policies_df])

    if len(all_policies_df) > 0:
        temp_csv_buffer = io.BytesIO()
        all_policies_df.to_csv(temp_csv_buffer, index=False)
        temp_csv_buffer.seek(0)
        out_filename = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{sub_type}_{DEST_FOLDER_LEVEL_5}_policy_extraction.csv'
        s3_client.upload_fileobj(temp_csv_buffer, bucket_name, out_filename)
        print(f'policy extract file uploaded to s3 bucket {bucket_name} after comparing policies')
    else:
        all_policies_df['extrctn_status'] = 'no change'

    send_email_via_ses(f'{sub_type.upper()} Payment Policies Extraction Summary',
                       html_body,
                       sender_email='rightpx@zignaai.com',
                       recipient_email=['suriya@zignaai.com','netra@zignaai.com', 'gopireddy@zignaai.com', 'connie@zignaai.com', 'sarvesh@zignaai.com'])


    batch_size = 100
    all_policies_df = all_policies_df[~all_policies_df['extrctn_status'].isin(['removed'])] 
    
    print("column names of all_policies_df are: ", all_policies_df.columns)
    print("The first element of client name column is: ", str(all_policies_df['extrctn_client_name'].unique()[0]))
    if str(all_policies_df['extrctn_client_name'].unique()[0]) in ['uhc', 'centene']:
        all_policies_df = all_policies_df[['extrctn_client_name', 'extrctn_pdf_links']]
        all_policies_df = all_policies_df.drop_duplicates()

    print("All policies length: ",len(all_policies_df))
    batches = [all_policies_df[idx:idx+batch_size] for idx in range(0, len(all_policies_df), batch_size)]
    print('Total batches',len(batches))
    for i, batch in enumerate(batches):
        batch_key = f'{DEST_FOLDER_LEVEL_1}/{DEST_FOLDER_LEVEL_2}/{PAYOR_NAME_DEST_FOLDER_LEVEL_3}/{DEST_FOLDER_LEVEL_4}/{DEST_FOLDER_LEVEL_5}/{DEST_FOLDER_LEVEL_6}/{DEST_FOLDER_LEVEL_7}/{i}_{sub_type}_{DEST_FOLDER_LEVEL_5}_batch_policies_download.csv'
        batch_csv_buffer = io.BytesIO()
        batch.to_csv(batch_csv_buffer, index=False)
        batch_csv_buffer.seek(0)
        s3_client.upload_fileobj(batch_csv_buffer, bucket_name, batch_key)
        print(f'policy extract batch {i} file uploaded to s3 bucket {bucket_name} after comparing policies')
        print(f"Batch {i} uploaded to s3")