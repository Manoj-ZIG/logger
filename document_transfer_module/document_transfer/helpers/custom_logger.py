
import builtins
import csv
import os
from datetime import datetime
import re

LOG_FILE = "/Users/manojkumar.nagula/Downloads/custom_losgs.csv"

category={
    'Received file':'File received',
    'query for all related ARLs':'Finding all related ARLs',
    'renamed MR name for':'MR renamed',
    'duplication check':'Duplication check',
    'MR is a duplicate':'Duplication check',
    'MR is not a duplicate':'Duplication check',
    'text extraction':'Text extraction',
    'PHI validation':'PHI check',
    'file went to manual review because of PHI':'PHI check',
    'digitization check':'Digitization check',
    'text extaction started':'Textraction',
    'copying file to raw':'Moving to processed',
    'copied file to raw folder':'Moving to processed',
    'validation query':'Claim level validation',
    'checking claim_id existence':'claim_id check in transformed_claims',
    'claim_id found':'claim_id check in transformed_claims',
    'claim_id not found':'claim_id check in transformed_claims',
    'Checking if document is greater than 75 pages':'Required sections check',
    'Document is greater than 75 pages':'Required sections check',
    'Document is less than 75 pages':'Required sections check',
    'pdf is less than 75 pages moved to manual_review':'Required sections check',
    'Chunks Generated':'Chunks generated',
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

if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp","ARL","File","Category","Message"])

def custom_print(*args, **kwargs):
    timestamp = datetime.now().isoformat(timespec='milliseconds')
    message = " ".join(str(arg) for arg in args)
    # formatted_message = f"{timestamp}   {message}"

    
    builtins._original_print(message, **kwargs)

    
    if ":-" in message:
        with open(LOG_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)

            pattern = r'\b(AJX[\w\-]{7}|H00[\w\-]{9}|V00[\w\-]{9}|200[\w\-]{4})'
            ARL = re.findall(pattern, message, flags=re.IGNORECASE)            
            if not ARL:
                ARL = ''
            else:
                ARL = ARL[0]
            file = message.split(':-')[-1]
            for key in category:
                if key.lower() in message.lower():
                    category_ = category.get(key,"")
                    break
            Message = message.split(':-')[0]
            writer.writerow([timestamp,ARL,file,category_, Message])

def enable_custom_logging():
    builtins._original_print = builtins.print
    builtins.print = custom_print

def disable_custom_logging():
    builtins.print = builtins._original_print
