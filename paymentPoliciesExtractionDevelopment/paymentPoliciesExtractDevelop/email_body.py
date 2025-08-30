import boto3
from constants import SENDER_MAIL, RECIPIENTS_LIST

ses_client = boto3.client('ses', region_name='us-east-1')


def send_email_via_ses(subject, html_body,sender_email=SENDER_MAIL,recipient_email=RECIPIENTS_LIST):

    response = ses_client.send_email(
        Source=sender_email,
        Destination={
            'ToAddresses': recipient_email
        },
        Message={
            'Subject': {
                'Data': subject
            },
            'Body': {
                'Html': {
                    'Data': html_body
                }
            }
        }
    )
    return response


def email_body(PAYOR_NAME_DEST_FOLDER_LEVEL_3, bucket_name, download_date, df, error_df, out_filename):

    html_body = f"""
        start
        <p>Hi Team,</p>
        <p> Hope this email finds you well.</p>
        <h2><font color="purple">{PAYOR_NAME_DEST_FOLDER_LEVEL_3.upper()} Payment Policies Extraction Summary</font></h2>
        
            <p>Total <strong>{PAYOR_NAME_DEST_FOLDER_LEVEL_3}</strong> base urls used to extracted the policies <strong>{len(set(df['extrctn_url']))}</strong></p>
            <p>Total unique <strong>{PAYOR_NAME_DEST_FOLDER_LEVEL_3}</strong> policy links extracted <strong>{len(set(df['extrctn_pdf_links']))}</strong></p>

            <br>

            <p> To download the latest {PAYOR_NAME_DEST_FOLDER_LEVEL_3} payment policies, please click <a href="https://{bucket_name}.s3.amazonaws.com/{out_filename}">here</a></p>
            <p> For more queries, please reply to the sender of this email.</p>
            <br>
            <p>Regards</p>
            <p>Data Team - ZignaAI.</p>
        end
        """
    return html_body