import boto3
from datetime import date

COLUMN_PREFIX = 'dwnld_'

s3_client = boto3.client('s3', region_name='us-east-1')
DOWNLOAD_BUCKET_NAME = 'zai-reference-data'

DEST_FOLDER_LEVEL_1 = 'zigna'
DEST_FOLDER_LEVEL_2 = 'policy_catalog'
DEST_FOLDER_LEVEL_4 = 'production_data'
DEST_FOLDER_LEVEL_5 =  date.today()
DEST_FOLDER_LEVEL_6 =  'download'
EXTRACT_FOLDER = 'extraction'
DEST_FOLDER_LEVEL_7 = 'batch'
DEST_FOLDER_LEVEL_8 = 'policies'
DEST_FOLDER_LEVEL_9 = 'policies_download_status'
DEST_FOLDER_LEVEL_10 = 'policies_abstract_trigger'