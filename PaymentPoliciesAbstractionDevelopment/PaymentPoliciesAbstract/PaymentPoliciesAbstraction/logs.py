import logging
from datetime import datetime
from constants import s3_client, BUCKET_NAME, RAW_DEST_FOLDER_LEVEL_1, RAW_DEST_FOLDER_LEVEL_2 , RAW_DEST_FOLDER_LEVEL_4 , RAW_DEST_FOLDER_LEVEL_5 , RAW_DEST_FOLDER_LEVEL_6 , RAW_DEST_FOLDER_LEVEL_7 

formatted_datetime = datetime.now().strftime("%Y_%m_%d_%H%M%S")

class CustomLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Set the default log level
        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(lineno)d | %(message)s')
        file_handler = logging.FileHandler(f'{payor_name}_abstraction_logs_{formatted_datetime}.log')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        try:             
            self.s3_client.upload_file(f'{payor_name}_abstraction_logs_{formatted_datetime}.log', BUCKET_NAME, f'{RAW_DEST_FOLDER_LEVEL_1}/{RAW_DEST_FOLDER_LEVEL_2}/{payor_name}/{RAW_DEST_FOLDER_LEVEL_4}/{RAW_DEST_FOLDER_LEVEL_5}/{RAW_DEST_FOLDER_LEVEL_6}/log_files/{payor_name}_abstraction_logs_{formatted_datetime}.log')             
            print("Log file uploaded to S3 successfully.")         
        except Exception as e:             
            print(f"Error uploading log file to S3: {e}")
# Initialize the logger at the start of your main script
common_logger = CustomLogger(__name__).logger


# print(common_logger.info(0, 'hgg'))