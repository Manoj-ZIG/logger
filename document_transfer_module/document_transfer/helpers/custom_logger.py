import builtins
import os
import re
from datetime import datetime
from io import BytesIO
import boto3

CATEGORY_MAP = {
    'Received file':'File received',
    'query for all related ARLs':'Finding all related ARLs',
    'renamed MR name for':'MR renamed',
    'duplication check':'Duplication check',
    'MR is a duplicate':'Duplication check',
    'MR is not a duplicate':'Duplication check',
    'text extraction':'Text extraction',
    'PHI validation':'PHI check',
    'manual review for PHI mismatch':'PHI check',
    'digitization check':'Digitization check',
    'corrupted pdf':'Document corrupted',
    'copying file to raw':'Moving to processed',
    'copied file to raw folder':'Moving to processed',
    'validation query':'Claim level validation',
    'checking claim_id existence':'Claim level validation',
    'claim_id found':'Claim level validation',
    'claim_id not found':'Claim level validation',
    'Checking if document is greater than 75 pages':'Required sections check',
    'Document is greater than 75 pages':'Required sections check',
    'Document is less than 75 pages':'Required sections check',
    'pdf is less than 75 pages moved to manual_review':'Required sections check',
    'Chunks Generated':'Chunking',
    'Json to CSV':'Json to CSV',
    'Textraction started':'Text extraction',
    'checking if patient DOB and Name':'PHI check',
    'patient DOB':'PHI check',
    'patient Name':'PHI check',
    'Collecting all possible ARLs':'PHI check',
    'copying pdf to processed':'Moving to processed',
    'copied pdf to processed':'Moving to processed',
    'copying pdf to error':'Error while copying pdf',
    'No of pages:':'Total pages',
    'Txt manifest file':'Loading into Manifest',
    'Moving pdf to manual review QA':'Manual review because of small size',
    'Moved pdf to manual review QA':'Manual review because of small size',
    'Moved pdf to raw folder': 'Validation completed yet to digitize',
    'PDF chunking':'Chunking'
}

class S3Logger:
    def __init__(self, category_map=CATEGORY_MAP, s3_folder="execution_trace_logs"):
        self._original_print = builtins.print
        self.category_map = category_map
        self.s3_folder = s3_folder
        self.s3_bucket = os.environ.get('PARAMETERS_BUCKET_NAME', '')
        self.s3_client = self._init_s3_client()
        self.enable()

    def _init_s3_client(self):
        try:
            client = boto3.client('s3', region_name='us-east-1')
            client.list_buckets()
            self._original_print("S3 client initialized successfully using IAM role.")
            return client
        except Exception as e:
            self._original_print(f"Failed to initialize S3 client with IAM role: {str(e)}")
            try:
                from constants.aws_config import aws_access_key_id, aws_secret_access_key
            except:
                from ..constants.aws_config import aws_access_key_id, aws_secret_access_key
            return boto3.client('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

    def enable(self):
        builtins._original_print = builtins.print
        builtins.print = self._custom_print

    def disable(self):
        builtins.print = self._original_print

    def _custom_print(self, *args, **kwargs):
        timestamp = datetime.now().isoformat(timespec='milliseconds')
        message = " ".join(str(arg) for arg in args)
        self._original_print(message, **kwargs)

        if ":-" not in message:
            return

        ARL = self._extract_arl(message)
        client = self._determine_client(ARL)
        file_name_part = message.split(':-')[-1].strip()
        Message = message.split(':-')[0].strip()
        category_ = self._match_category(message)

        if not category_ or not client:
            return

        content = self._prepare_content(timestamp, ARL, file_name_part, category_, Message)
        self._upload_to_s3(client, file_name_part, category_, content)

    def _extract_arl(self, message):
        pattern = r'(AJX[\w\-]{7}|H00[\w\-]{9}|V00[\w\-]{9}|200[\w\-]{4})'
        match = re.findall(pattern, message, flags=re.IGNORECASE)
        return match[0].upper() if match else ''

    def _determine_client(self, arl):
        if arl.startswith("AJX"):
            return "devoted"
        elif arl.startswith(("H00", "V00", "22")):
            return "helix"
        return ""

    def _match_category(self, message):
        for key in self.category_map:
            if key.lower() in message.lower():
                return self.category_map[key]
        return ""

    def _prepare_content(self, timestamp, arl, file_name, category, message):
        headers = "timestamp|lambda|arl|file|category|message"
        values = f"{timestamp}|document_transfer_lambda|{arl}|{file_name}|{category}|{message}"
        return f"{headers}\n{values}"

    def _upload_to_s3(self, client, file_name, category, content):
        buffer = BytesIO()
        buffer.write(content.encode('utf-8'))
        buffer.seek(0)

        date_folder = datetime.now().strftime("%Y%m%d")
        timestamp_for_file = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{client}/{self.s3_folder}/{date_folder}/{file_name.replace('.pdf','')}_{category}_{timestamp_for_file}.txt"

        try:
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=buffer)    
            buffer.close()
        except Exception as e:
            self._original_print(f"Failed to upload TXT to S3: {e}")
