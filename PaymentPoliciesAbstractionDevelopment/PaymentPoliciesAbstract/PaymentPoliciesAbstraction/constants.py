import boto3
import pandas as pd
import re
from datetime import datetime,date
import requests
import json

s3_client = boto3.client('s3', region_name='us-east-1')
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
ssm = boto3.client('ssm')

DOWNLOAD_BUCKET_NAME = 'zai-reference-data'
APPLICATION_DEST_FOLDER_LEVEL_2 = "policy_catalog"
APPLICATION_DEST_FOLDER_LEVEL_3 = "approved_policies"
formatted_datetime = date.today()
RAW_DEST_FOLDER_LEVEL_1 = "zigna"
RAW_DEST_FOLDER_LEVEL_2 = "policy_catalog"
RAW_DEST_FOLDER_LEVEL_4 = "production_data"
RAW_DEST_FOLDER_LEVEL_5 = date.today()
RAW_DEST_FOLDER_LEVEL_6 = "abstraction"
RAW_DEST_FOLDER_LEVEL_7 = "raw"
DOWNLOAD_FOLDER = 'download'

def get_auth_token(secretpath):
    auth_token = ''
    secret_name = secretpath
    try:
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_values = response['SecretString']
        credentials = json.loads(secret_values)
        print("Fetching credentials from secret manager successful")
        userId = credentials['userId']
        endpoint = credentials['endpoint']
        apiKey = credentials['apiKey']
        headers = {
        "X-API-Key": apiKey
        }

        payload = {
            "email": userId
            }
        res = requests.post(f"{endpoint}/getAuthKeyToken", json = payload, headers = headers)
        auth_token = res.json()["accessToken"]
        print("Fetching auth token successful")
    except Exception as e:
        print(f"Error during api call: {e}")
    return auth_token

devoted_base_url  =r'https://www.devoted.com/providers/payment-policies/'
devoted_state_mapping_dictionary = {'Alabama': 'AL', 'Arizona': 'AR', 'Colorado': 'CO', 'Florida': 'FL', 'Hawaii': 'HI', 'Illinois': 'IL', 'North Carolina': 'NC',
                                    'Ohio': 'OH', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'South Carolina': 'SC', 'Tennessee': 'TN', 'Texas': 'TX'}
devoted_states_list = ['AL', 'AR', 'CO', 'FL', 'HI', 'IL', 'NC', 'OH', 'OR', 'PA', 'SC', 'TN', 'TX']

service_level_dict = {'cms1500': 'Professional', 'cms 1500': 'Professional', 'cms1450':'Facility', 'cms 1450':'Facility',
                      'ub04':'Facility', 'ub 04':'Facility', 'ipps':'Facility', 'opps':'Facility', 'place of service 19':'Professional'}

date_formats = {
            r"\d{1,4}[-]\d{1,2}[-]\d{4}" : '%m-%d-%Y',
            r"\d{1,4}[-]\d{1,2}[-]\d{2}" : '%m-%d-%y',

            r"\d{1,4}[.]\d{1,2}[.]\d{4}" : '%m.%d.%Y',
            r"\d{1,4}[.]\d{1,2}[.]\d{2}" : '%m.%d.%y',

            r"\d{1,4}[/]\d{1,2}[/]\d{4}" : '%m/%d/%Y',
            r"\d{1,4}[/]\d{1,2}[/]\d{2}" : '%m/%d/%y',

            r"\d{1,4}[,]\d{1,2}[,]\d{4}" : '%m,%d,%Y',
            r"\d{1,4}[,]\d{1,2}[,]\d{2}" : '%m,%d,%y',

            r"\d{1,4}[ ]\d{1,2}[ ]\d{4}" : '%m %d %Y',
            r"\d{1,4}[ ]\d{1,2}[ ]\d{2}" : '%m %d %y',

            r"\d{1,4}[_]\d{1,2}[_]\d{4}" : '%m_%d_%Y',
            r"\d{1,4}[_]\d{1,2}[_]\d{2}" : '%m_%d_%y',
            
            r"\d{1,4}[:]\d{1,2}[:]\d{4}" : '%m:%d:%Y',
            r"\d{1,4}[:]\d{1,2}[:]\d{2}" : '%m:%d:%y',

            r"\d{1,4}[|]\d{1,2}[|]\d{4}" : '%m|%d|%Y',
            r"\d{1,4}[|]\d{1,2}[|]\d{2}" : '%m|%d|%y',

            r"\d{1,2}[.]\d{1,4}[.]\d{4}" : '%d.%m.%Y',
            r"\d{1,2}[.]\d{1,4}[.]\d{2}" : '%d.%m.%y',

            r"\d{4}[-]\d{1,4}" : '%Y-%m',
            r"\d{2}[-]\d{1,4}" : '%y-%m',

            r"\d{4}[.]\d{1,4}" : '%Y.%m',
            r"\d{2}[.]\d{1,4}" : '%y.%m',

            r"\d{4}[/]\d{1,4}" : '%Y/%m',
            r"\d{2}[/]\d{1,4}" : '%y/%m',

            r"\d{4}[,]\d{1,4}" : '%Y,%m',
            r"\d{2}[,]\d{1,4}" : '%y,%m',

            r"\d{4}[ ]\d{1,4}" : '%Y %m',
            r"\d{2}[ ]\d{1,4}" : '%y %m',

            r"\d{4}[_]\d{1,4}" : '%Y_%m',
            r"\d{2}[_]\d{1,4}" : '%y_%m',

            r"\d{4}[:]\d{1,4}" : '%Y:%m',
            r"\d{2}[:]\d{1,4}" : '%y:%m',

            r"\d{4}[|]\d{1,4}" : '%Y|%m',
            r"\d{2}[|]\d{1,4}" : '%y|%m', 

            r"\d{1,4}[-]\d{4}" : '%m-%Y',
            r"\d{1,4}[-]\d{2}" : '%m-%y',

            r"\d{1,4}[.]\d{4}" : '%m.%Y',
            r"\d{1,4}[.]\d{2}" : '%m.%y',

            r"\d{1,4}[/]\d{4}" : '%m/%Y',
            r"\d{1,4}[/]\d{2}" : '%m/%y',

            r"\d{1,4}[,]\d{4}" : '%m,%Y',
            r"\d{1,4}[,]\d{2}" : '%m,%y',

            r"\d{1,4}[ ]\d{4}" : '%m %Y',
            r"\d{1,4}[ ]\d{2}" : '%m %y',

            r"\d{1,4}[_]\d{4}" : '%m_%Y',
            r"\d{1,4}[_]\d{2}" : '%m_%y',

            r"\d{1,4}[:]\d{4}" : '%m:%Y',
            r"\d{1,4}[:]\d{2}" : '%m:%y',

            r"\d{1,4}[|]\d{4}" : '%m|%Y',
            r"\d{1,4}[|]\d{2}" : '%m|%y',

            r"\b\d{4}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?" : '%Y %B %d',
            r"\b\d{2}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?" : '%y %b %d',
            r"\b\d{4}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?" : '%Y %b %d',
            r"\b\d{2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?" : '%y %B %d',
            r"\b\d{4}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{1,4}(?:st|nd|rd|th)?" : '%Y %B, %d',
            r"\b\d{2}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{1,4}(?:st|nd|rd|th)?" : '%y %b, %d',
            r"\b\d{4}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{1,4}(?:st|nd|rd|th)?" : '%Y %b, %d',
            r"\b\d{2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{1,4}(?:st|nd|rd|th)?" :'%y %B, %d',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}" : '%d %B %Y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{4}" : '%d %b %Y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{2}" : '%d %B %y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{2}" : '%d %b %y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{4}" : '%d %B, %Y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{4}" : '%d %b, %Y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{2}" : '%d %B, %y',
            r"\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{2}" : '%d %b, %y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{4}\b" : '%B %d, %Y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{4}\b" : '%b %d, %Y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2}\b" : '%B %d, %y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2}\b" : '%b %d, %y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2}\b" : '%b %d %y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{4}\b" : '%b %d %Y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2}\b" : '%B %d %y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{4}\b" : '%B %d %Y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}" : '%B %Y',
            r"\b(?:JJan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{4}" : '%b %Y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{2}" : '%B %y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)\s+\d{2}" : '%b %y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{4}" : '%B, %Y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{4}" : '%b, %Y',
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December),\s+\d{2}" : '%B, %y',
            r"\b(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec),\s+\d{2}" : '%b, %y',
            r"\b\d{4}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)" : '%Y %B',
            r"\b\d{2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)" : '%y %B',
            r"\b\d{4}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)" : '%Y %b',
            r"\b\d{2}\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)" : '%y %b',
            r"\b\d{4},\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)" : '%Y, %B',
            r"\b\d{2},\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)" : '%y, %B',
            r"\b\d{4},\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)" : '%Y, %b',
            r"\b\d{2},\s+(?:Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)" : '%y, %b'    
            }

COLUMN_PREFIX = 'abstrctn_'


def add_column_prefix(df, COLUMN_PREFIX):
    return [COLUMN_PREFIX + col for col in df.columns]

cms_mcd_ncd_service_level_dict = {
                    "physicians' services": 'All',
                    "incident to a physician's professional service": 'Professional',
                    "inpatient hospital services": 'Facility',
                    "outpatient hospital services": 'Facility',
                    "diagnostic laboratory tests": 'All',
                    "durable medical equipment": 'All',
                    "incident to a physician''s service": 'Facility',
                    "X-ray, Radium, and Radioactive Isotope Therapy": 'Facility',
                    "x-ray, radium, and radioactive isotope therapy": 'Facility',
                    "transplantation services for esrd-entitled beneficiaries": 'Facility',
                    "surgical dressings": 'All',
                    "splints, casts, other devices used for reduction of fractures and dislocations": 'All',
                    "skilled nursing facility": 'Facility',
                    "shoes for patients with diabetes": 'All',
                    "self-care home dialysis support services": 'All',
                    "screening pelvic exam": 'All',
                    "screening pap smear": 'All',
                    "screening mammography": 'All',
                    "screening for glaucoma": 'All',
                    "rural health clinic services": 'Facility',
                    "religious nonmedical health care institution": 'Facility',
                    "qualified psychologist services": 'All',
                    "prosthetic devices": 'All',
                    "prostate cancer screening tests": 'All',
                    "post-institutional home health services": 'Professional',
                    "post-hospital extended care services": 'Professional',
                    "podiatrist services": 'Professional',
                    "pneumococcal vaccine and administration": 'All',
                    "physician assistant services": 'Professional',
                    "partial hospitalization services": 'Facility',
                    "outpatient occupational therapy services": 'Facility',
                    "outpatient hospital services incident to a physician''s service": 'Facility',
                    "osteoporosis drug": 'All',
                    "orthotics and prosthetics": 'All',
                    "oral antiemetic drugs": 'All',
                    "optometrist services": 'Professional',
                    "nurse practitioner services": 'Professional',
                    "medical nutrition therapy services": 'All',
                    "leg, arm, back, and neck braces \(orthotics\)": 'All',
                    "institutional dialysis services and supplies": 'Facility',
                    "inpatient psychiatric hospital services": 'Facility',
                    "influenza vaccine and administration": 'All',
                    "immunosuppressive drugs": 'All',
                    "hospice care": 'All',
                    "home health services": 'Professional',
                    "home dialysis supplies and equipment": 'All',
                    "hepatitis b vaccine and administration": 'All',
                    "federally qualified health center services": 'Facility',
                    "eyeglasses after cataract surgery": 'All',
                    "extended care services": 'All',
                    "erythropoietin for dialysis patients": 'All',
                    "diagnostic tests \(other\)": 'All',
                    "diagnostic services in outpatient hospital": 'Facility',
                    "diabetes outpatient self-management training": 'Facility',
                    "dentist services": 'Professional',
                    "critical access hospital services": 'Facility',
                    "comprehensive outpatient rehabilitation facility \(corf\) services": 'Facility',
                    "colorectal cancer screening tests": 'All',
                    "clinical social worker services": 'Professional',
                    "clinical nurse specialist services": 'Professional',
                    "chiropractor services": 'Professional',
                    "certified registered nurse anesthetist services": 'Professional',
                    "certified nurse-midwife services": 'Professional',
                    "bone mass measurement": 'All',
                    "blood clotting factors for hemophilia patients": 'All',
                    "audiology services": 'All',
                    "artificial legs, arms, and eyes": 'All',
                    "antigens": 'All',
                    "ambulatory surgical center facility services": 'Facility',
                    "ambulance services": 'Facility',
                    "no benefit category": 'All',
                    "outpatient physical therapy services": 'Facility',
                    "drugs and biologicals": 'All',
                    "outpatient speech language pathology services": 'Facility'
                    }


CMS_MCD_ZIP_URL = 'https://downloads.cms.gov/medicare-coverage-database/downloads/exports/all_data.zip'
CMS_MCD_FILE_NAME = CMS_MCD_ZIP_URL.split('/')[-1].replace('.zip','')

BASE_PATH = f"reference_files/CMS_MCD_POLICIES/{formatted_datetime}/{CMS_MCD_FILE_NAME}"

cms_mcd_articles_path = {"article": f"{BASE_PATH}/all_article/all_article_csv/article.csv",
                         "article_x_contractor": f"{BASE_PATH}/all_article/all_article_csv/article_x_contractor.csv",
                         "contractor": f"{BASE_PATH}/all_article/all_article_csv/contractor.csv",
                         "contractor_jurisdiction": f"{BASE_PATH}/all_article/all_article_csv/contractor_jurisdiction.csv",
                         "contractor_type_lookup": f"{BASE_PATH}/all_article/all_article_csv/contractor_type_lookup.csv",
                         "state_lookup": f"{BASE_PATH}/all_article/all_article_csv/state_lookup.csv",
                         "state_x_region": f"{BASE_PATH}/all_article/all_article_csv/state_x_region.csv",
                         "region_lookup": f"{BASE_PATH}/all_article/all_article_csv/region_lookup.csv",
                         "dmerc_region_lookup": f"{BASE_PATH}/all_article/all_article_csv/dmerc_region_lookup.csv",
                         "icd10_cov": f"{BASE_PATH}/all_article/all_article_csv/article_x_icd10_covered.csv",
                         "icd10_noncov": f"{BASE_PATH}/all_article/all_article_csv/article_x_icd10_noncovered.csv",
                         "hcpc": f"{BASE_PATH}/all_article/all_article_csv/article_x_hcpc_code.csv",
                         "code_table": f"{BASE_PATH}/all_article/all_article_csv/article_x_code_table.csv",
                         "hcpc_mod": f"{BASE_PATH}/all_article/all_article_csv/article_x_hcpc_modifier.csv",
                         "icd10_pcs": f"{BASE_PATH}/all_article/all_article_csv/article_x_icd10_pcs_code.csv",
                         "related_documents": f"{BASE_PATH}/all_article/all_article_csv/article_related_documents.csv"
}

cms_mcd_lcd_path =      {"lcd": f"{BASE_PATH}/all_lcd/all_lcd_csv/lcd.csv",
                         "lcd_x_contractor": f"{BASE_PATH}/all_lcd/all_lcd_csv/lcd_x_contractor.csv",
                         "contractor": f"{BASE_PATH}/all_lcd/all_lcd_csv/contractor.csv",
                         "contractor_jurisdiction": f"{BASE_PATH}/all_lcd/all_lcd_csv/contractor_jurisdiction.csv",
                         "contractor_type_lookup": f"{BASE_PATH}/all_lcd/all_lcd_csv/contractor_type_lookup.csv",
                         "state_lookup": f"{BASE_PATH}/all_lcd/all_lcd_csv/state_lookup.csv",
                         "state_x_region": f"{BASE_PATH}/all_lcd/all_lcd_csv/state_x_region.csv",
                         "region_lookup": f"{BASE_PATH}/all_lcd/all_lcd_csv/region_lookup.csv",
                         "dmerc_region_lookup": f"{BASE_PATH}/all_lcd/all_lcd_csv/dmerc_region_lookup.csv",
                         "hcpc": f"{BASE_PATH}/all_lcd/all_lcd_csv/lcd_x_hcpc_code.csv"
                         }

cms_mcd_ncd_path =      {"ncd_trkg": f"{BASE_PATH}/ncd/ncd_csv/ncd_trkg.csv",
                         "ncd_trkg_bnft_xref": f"{BASE_PATH}/ncd/ncd_csv/ncd_trkg_bnft_xref.csv",
                         "ncd_bnft_ctgry_ref": f"{BASE_PATH}/ncd/ncd_csv/ncd_bnft_ctgry_ref.csv"
                         }
    
document_type_map = {
                'Medical policy':  'Payment Policies',
                'Payment policy': 'Payment Policies',
                'Clinical Guideline': 'Clinical Guidelines',
                'Pharmacy policy': 'Pharmacy Policies',
                'Drug policy': 'Pharmacy Policies',
                'Clinical policy': 'Payment Policies',
                'Coding policy': 'Coding Policies'
}

affiliate_map = {
    'United Healthcare of Nevada' : 'Health Plan of Nevada Medicaid'
}

service_level_map = {
    'Facility': 'FACILITY',
    'Professional' : 'PROFESSIONAL'
}

lineofbusiness_map = {
    'Commercial':  'COMMERCIAL',
    'Medicare' : 'MEDICARE',
    'Medicaid': 'MEDICAID',
    'Medicare Advantage':  'MEDICARE',
    'Marketplace':  'MARKETPLACE'
}

contractor_name_map = {
    'Palmetto GBA': 'Palmetto GBA, LLC',
    'Novitas Solutions, Inc.': 'Novitas Solutions, INC',
    'Wisconsin Physicians Service Insurance Corporation': 'Wisconsin Physicians Service Government Health Administrators'
}

contract_type_map = {
    'MAC - Part A':  'A / B',
    'MAC - Part B' :  'A / B',
    'A and B MAC' :  'A / B',
    'DME MAC':  'DME',
    'HHH MAC':  'HH & H',
    'A and B and HHH MAC': 'A / B,HH & H'
}

states_map = {
        'Meichigan': 'Michigan','Meissouri':  'Missouri','Missouri - Entire State': 'Missouri','Missouri - Northwestern': 'Missouri',
        'California - Entire State': 'California','California - Northern' : 'California','California - Southern': 'California','New York - Entire State':  'New York',
        'New York - Downstate' : 'New York','New York - Upstate': 'New York','New York - Queens': 'New York','Northern Mariana Islands':  'North Mariana Islands',
        'Virgin Islands' : 'US Virgin Islands','AL':  'Alabama','AK' :  'Alaska','AZ': 'Arizona','AR':  'Arkansas','CA': 'California','CO': 'Colorado',
        'CT': 'Connecticut','DE':  'Delaware','FL' : 'Florida','GA': 'Georgia','HI': 'Hawaii','ID': 'Idaho','IA': 'Iowa','KS': 'Kansas','KY':  'Kentucky','LA' : 'Louisiana',
        'ME': 'Maine','MA': 'Massachusetts','MI': 'Michigan','MN': 'Minnesota','MS': 'Mississippi','MO':  'Missouri','MT' : 'Montana','NE': 'Nebraska',
        'NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey','AS': 'American Samoa','DC': 'District of Columbia','GU': 'Guam','IL': 'Illinois',
        'IN': 'Indiana','MD': 'Maryland', 'NM':  'New Mexico', 'NY': 'New York','NC':  'North Carolina','ND' : 'North Dakota','MP': 'North Mariana Islands',
        'OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','PR':  'Puerto Rico','RI' : 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota',
        'TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VT':  'Vermont','VA' : 'Virginia','WA': 'Washington','WV': 'West Virginia','WI': 'Wisconsin','WY': 'Wyoming','VI': 'US Virgin Islands'
        }

def replace_keys_with_ids(df, column, key_id_dict):
    key_id_dict_lower = {k.lower(): v for k, v in key_id_dict.items()}
    if "A / B,HH & H" in set(df[column]):
        key_id_dict_lower.update({"a / b,hh & h":"1,2"})
    print(f"lower dict : {key_id_dict_lower}")
    print(f"unique col values: {set(df[column])}")
    df[column] = (df[column].str.lower().replace(key_id_dict_lower)).astype(str)
    return df
    
def get_id(api, token, category, labelname, label):
    response = requests.get(f"{api}{category}?{labelname}={label}", headers={"Authorization": f"Bearer {token}"})
    print("status is:", response.status_code)
    if (response.status_code == 200) and (response.text != 'null'):
        return str(json.loads(response.text)[0]["id"])
    else:
        print("API ERROR: ", response.status_code, response.text, category, label)
        return "API error"
    
def all_ids_per_category(api, category, token):
    response = requests.get(f"{api}{category}", headers={"Authorization": f"Bearer {token}"})
    print('Category: ', category, ' Status code: ', response.status_code)
    if (response.status_code == 200) and (response.text != 'null'):
        dict_lst = str(json.loads(response.text))
        dict_lst = eval(dict_lst)
        df=pd.DataFrame(dict_lst)
        l1 = df[df.columns[0]]
        l2 = df[df.columns[1]]
        tuples = [(key, value)
                for i, (key, value) in enumerate(zip(l2, l1))]
        res = dict(tuples)
        for keys in res:
            res[keys] = str(res[keys])
        return res
    else:
        print("API ERROR: ", response.status_code, response.text, category)
        return {}

def payor_id_mapping_fn(api, token):
    payor_id_mapping =      {
                            'CMS': get_id(api, token, 'payor_info', 'key', 'cms'),
                            'Devoted': get_id(api, token, 'payor_info', 'key','devoted'),
                            'UHC': get_id(api, token, 'payor_info', 'key','uhc')
                            }
    return payor_id_mapping

# def document_type_id_mapping_fn(api, token):
#     document_type_id_mapping = {
#                 'Medical policy': get_id(api, token, 'policy_types', 'label', 'Payment Policies'),
#                 'Payment policy': get_id(api, token, 'policy_types', 'label','Payment Policies'),
#                 'Clinical Guideline': get_id(api, token, 'policy_types', 'label','Clinical Guidelines'),
#                 'Pharmacy policy': get_id(api, token, 'policy_types', 'label','Pharmacy Policies'),
#                 'Drug policy': get_id(api, token, 'policy_types', 'label','Pharmacy Policies'),
#                 'Clinical policy': get_id(api, token, 'policy_types', 'label','Payment Policies'),
#                 'Coding policy': get_id(api, token, 'policy_types', 'label','Coding Policies'),
#                 'Bulletins & Updates': get_id(api, token, 'policy_types', 'label','Bulletins and Updates'),
#                 'CMS NCD': get_id(api, token, 'policy_types', 'label','cms_ncd'),
#                 'CMS LCD': get_id(api, token, 'policy_types', 'label','cms_lcd'),
#                 'CMS Articles': get_id(api, token, 'policy_types', 'label','cms_articles')
#                 } 
#     return document_type_id_mapping

# def affiliate_payor_id_mapping_fn(api, token):
#     affiliate_payor_id_mapping =      {
#                             'All Savers Health Plan': get_id(api, token, 'affiliate_payor_info', 'key', 'all-savers'),
#                             'Surest': get_id(api, token, 'affiliate_payor_info', 'key', 'surest'),
#                             'Heritage/UHC of the River Valley': get_id(api, token, 'affiliate_payor_info', 'key', 'heritage-uhc'),
#                             'Oxford Health Plan': get_id(api, token, 'affiliate_payor_info', 'key', 'oxford-health-plan'),
#                             'Mid-Atlantic Health Plan' : get_id(api, token, 'affiliate_payor_info', 'key', 'mid-atlantic'),
#                             'UHC MD IPA Plan' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-md-ipa'),
#                             'Optimum Choice' : get_id(api, token, 'affiliate_payor_info', 'key', 'optimum-choice'),
#                             'The Empire Plan' : get_id(api, token, 'affiliate_payor_info', 'key', 'the-empire-plan'),
#                             'UHC West Benefit' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-west-benefit'),
#                             'UHC West Medical' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-west-medical'),
#                             'UMR' : get_id(api, token, 'affiliate_payor_info', 'key', 'umr'),
#                             'Government Employees Health Association (GEHA)' : get_id(api, token, 'affiliate_payor_info', 'key', 'geha'),
#                             'Health Plan of Nevada Medicaid' : get_id(api, token, 'affiliate_payor_info', 'key', 'nv-medicaid'),
#                             'United Healthcare of Nevada' : get_id(api, token, 'affiliate_payor_info', 'key', 'nv-medicaid'),
#                             'UHC Community Plan of Indiana' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-in'),
#                             'UHC Community Plan of Kentucky' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-ky'),
#                             'UHC Community Plan of Louisiana' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-la'),
#                             'UHC Community Plan of Mississippi' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-ms'),
#                             'UHC Community Plan of North Carolina' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-nc'),
#                             'UHC Community Plan of Ohio' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-oh'),
#                             'UHC Community Plan of Pennsylvania' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-pa'),
#                             'UHC Community Plan of Tennessee' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-tn'),
#                             'UHC Community Plan of New Mexico' : get_id(api, token, 'affiliate_payor_info', 'key', 'uhc-nm'),
#                             'Rocky Mountain Health Plan' : get_id(api, token, 'affiliate_payor_info', 'key', 'rm-health-plan')
#                             }
#     return affiliate_payor_id_mapping

# def service_level_id_mapping_fn(api, token):
#     service_level_id_mapping =      {
#                             'Facility': get_id(api, token, 'service_level_info', 'label', 'FACILITY'),
#                             'Professional' : get_id(api, token, 'service_level_info', 'label', 'PROFESSIONAL')
#                             }
#     return service_level_id_mapping

# def line_of_business_id_mapping_fn(api, token):
#     line_of_business_id_mapping =      {
#                             'Commercial': get_id(api, token, 'line_of_business_info', 'label', 'COMMERCIAL'),
#                             'Medicare' :get_id(api, token, 'line_of_business_info', 'label', 'MEDICARE'),
#                             'Medicaid':get_id(api, token, 'line_of_business_info', 'label', 'MEDICAID'),
#                             'Medicare Advantage': get_id(api, token, 'line_of_business_info', 'label', 'MEDICARE'),
#                             'Marketplace': get_id(api, token, 'line_of_business_info', 'label', 'MARKETPLACE')
#     }
#     return line_of_business_id_mapping

# def state_id_mapping_fn(api, token):
#     state_id_mapping =      {
#         'Meichigan': 'Michigan','Meissouri':  'Missouri','Missouri - Entire State': 'Missouri','Missouri - Northwestern': 'Missouri',
#         'California - Entire State': 'California','California - Northern' : 'California','California - Southern': 'California','New York - Entire State':  'New York',
#         'New York - Downstate' : 'New York','New York - Upstate': 'New York','New York - Queens': 'New York','Northern Mariana Islands':  'North Mariana Islands',
#         'Virgin Islands' : 'US Virgin Islands','AL':  'Alabama','AK' :  'Alaska','AZ': 'Arizona','AR':  'Arkansas','CA': 'California','CO': 'Colorado',
#         'CT': 'Connecticut','DE':  'Delaware','FL' : 'Florida','GA': 'Georgia','HI': 'Hawaii','ID': 'Idaho','IA': 'Iowa','KS': 'Kansas','KY':  'Kentucky','LA' : 'Louisiana',
#         'ME': 'Maine','MA': 'Massachusetts','MI': 'Michigan','MN': 'Minnesota','MS': 'Mississippi','MO':  'Missouri','MT' : 'Montana','NE': 'Nebraska',
#         'NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey','AS': 'American Samoa','DC': 'District of Columbia','GU': 'Guam','IL': 'Illinois',
#         'IN': 'Indiana','MD': 'Maryland', 'NM':  'New Mexico', 'NY': 'New York','NC':  'North Carolina','ND' : 'North Dakota','MP': 'North Mariana Islands',
#         'OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','PR':  'Puerto Rico','RI' : 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota',
#         'TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VT':  'Vermont','VA' : 'Virginia','WA': 'Washington','WV': 'West Virginia','WI': 'Wisconsin','WY': 'Wyoming','VI': 'US Virgin Islands'
#         }
#     return state_id_mapping

# def contractor_name_id_mapping_fn(api, token):
#     contractor_name_id_mapping = {
#                             'Palmetto GBA': get_id(api, token, 'contractor_info', 'key', 'palmetto_gba'),
#                             'National Government Services, Inc.' : get_id(api, token, 'contractor_info', 'key', 'national_ngs'),
#                             'Noridian Healthcare Solutions, LLC' : get_id(api, token, 'contractor_info', 'key', 'noridian'),
#                             'Novitas Solutions, Inc.': get_id(api, token, 'contractor_info', 'key', 'novartis_solutions'),
#                             'CGS Administrators, LLC': get_id(api, token, 'contractor_info', 'key', 'cgs_administrators'),
#                             'First Coast Service Options, Inc.':get_id(api, token, 'contractor_info', 'key', 'first_coast'),
#                             'Wisconsin Physicians Service Insurance Corporation':get_id(api, token, 'contractor_info', 'key', 'wisconsin_gha')
#                             } 
#     return contractor_name_id_mapping

# def contract_type_id_mapping_fn(api, token):
#     contract_type_id_mapping = {
#                             'MAC - Part A': get_id(api, token, 'contract_type_info', 'key', 'a_and_b'),
#                             'MAC - Part B' : get_id(api, token, 'contract_type_info', 'key', 'a_and_b'),
#                             'A and B MAC' : get_id(api, token, 'contract_type_info', 'key', 'a_and_b'),
#                             'DME MAC': get_id(api, token, 'contract_type_info', 'key', 'dme'),
#                             'HHH MAC': get_id(api, token, 'contract_type_info', 'key', 'hh_and_h'),
#                             'A and B and HHH MAC':f"{get_id(api, token, 'contract_type_info', 'key', 'a_and_b')}, {get_id(api, token, 'contract_type_info', 'key', 'hh_and_h')}"
#                             } 
#     return contract_type_id_mapping

policy_type_list = ['Claims and Payment Policy:', 'Claims Payment Policy:', 'Concert Genetic Testing:', 
                    'Clinical practice guideline:', 'Research Clinical Care:', 'Concert Genetics Oncology:', 
                    'Payment Integrity Policy:', 'Genetic testing:', 'Concert Oncology:', 'Drug Policy:', 'State Policy:',
                    'Payment Policy:', 'Pharmacy Policy:', 'Clinical Policy:', 'Clinical  Policy:', 'Transparency Policy:',
                    'Behavioral Policy:', 'Vision Policy:', 'Practice Guidelines:']

policy_types_titles = ['DEPARTMENT:', 'BUSINESS UNIT:']
policy_names_titles= ['DOCUMENT NAME:', 'POLICY NAME:']
last_review_date_matches = ['Last Review Date:', 'Date of Last Review:',
                         'Date of Approval by Committee:', 'Date of Last Revision:',
                         'Date ofLast revision:','APPROVED DATE:', 'Date Updated in Database:', 'Revised Date(s):',
                         'Date of last revison:', 'last date reviewed:', 'Date of revision:', 'Amended Date:']
effective_date_matches = ['Revised Effective Date(s):', 'Effective Date:',
                          'Date effective:', 'Effective:', 'Original Effective Date:']
policy_number_list = ['Reference Number:','REFERENCE NUMBER:', 'POLICY ID:', 'P&P NUMBER:', 'Policy Number:']  
policy_number_pattern = '[A-Za-z]{1,4}\\-[A-Za-z|0-9]{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}(\\.|\\/)[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}\\.\\d{1,4}|[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.[A-Za-z]{1,4}\\.\\d{1,4}\\.\\d{1,4}'
lob_to_match = ['Line of Business:', 'LINE OF BUSINESS:']