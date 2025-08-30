import json
import devoted
import cms_mcd
import uhc
import transmittals
import centene_new
from constants import devoted_base_url,CMS_MCD_BASE_URLS,CMS_MCD_ZIP_URL,uhc_base_urls,centene_dict_new
import pandas as pd
import io
from datetime import date
import os

download_date =  date.today()
def lambda_handler(event, context):
    BUCKET_1_NAME = os.environ['BUCKET_NAME']
    API_1_NAME = os.environ['API']
    devoted.save_policy_files_to_s3(devoted_base_url, BUCKET_1_NAME, API_1_NAME)
    cms_mcd.save_policy_files_to_s3(CMS_MCD_ZIP_URL,CMS_MCD_BASE_URLS, BUCKET_1_NAME, API_1_NAME)
    # uhc.save_policy_files_to_s3(uhc_base_urls, BUCKET_1_NAME, API_1_NAME)
    # transmittals.save_policy_files_to_s3(BUCKET_1_NAME, API_1_NAME)
    # centene_new.save_policy_files_to_s3(centene_dict_new, BUCKET_1_NAME, API_1_NAME)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }
