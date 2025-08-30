import pandas as pd
import re
from datetime import datetime
from constants import date_formats,s3_client
import boto3
import io
import pikepdf 
from pdfminer.high_level import extract_text
from io import BytesIO, StringIO
from decimal import Decimal

sections_list = ["Table of Contents", "Application", "Reimbursement Guidelines", "Definitions", "Anatomic Modifiers",
                 "Questions and Answers", "Codes", "Resources", "Attachments", "Attachments: Please right click on the icon to open the file",
                 "State Exceptions", "Coverage Rationale", "Applicable Codes", "Policy","Exceptions", "To Review Guidelines",
                 "References", "Description of Services", "Clinical Evidence",
                 "U.S. Food and Drug Administration (FDA)", "Background", "Benefit Considerations",
                 "Centers for Medicare & Medicaid Services (CMS)", "Coverage Guidlines", "Documentation Requirements",
                 "History", "Policy History/Revision Information", "Guidline History/Revision Information", 'Policy approval and history:', 'Policy History',
                 'Applicable States', 'document history'
                 ]

history_section_list = ["History", "Policy History/Revision Information", "Guidline History/Revision Information"]


remove_list = ["Table of Contents", "Application", "Reimbursement Guidelines", "Definitions", "Anatomic Modifiers",
               "Questions and Answers", "Codes", "Resources", "History", "Attachments", "Attachments: Please right click on the icon to open the file",
               "State Exceptions", "Coverage Rationale", "Applicable Codes", "History","Exceptions", "To Review Guidelines",
               "Policy History/Revision Information", "References", "Guidline History/Revision Information",
               "Description of Services", "Clinical Evidence",
               "U.S. Food and Drug Administration (FDA)", "Background", "Benefit Considerations",
               "Centers for Medicare & Medicaid Services (CMS)", "Coverage Guidlines", "Documentation Requirements", 'document history'
               ]

sub_section_list = ["Reimbursement Guidelines"]


section_mapper = {"Attachments: Please right click on the icon to open the file": "Attachments"}

policy_type_list = ["Reimbursement Policy", "Medical Policy", "Medical Necessity", "Drug Policy", "Clinical Policy",
                    "UnitedHealthcare Oxford Clinical and Administrative Policies",
                    "Clinical and Administrative Policies", "Administrative Policy",
                    "Medical Benefit Drug Policy", "UnitedHealthcare West Benefit Interpretation Policy",
                    "UnitedHealthcare West Medical Management Guideline", 
                    "Coverage Determination Guideline","Clinical Guidelines",'Clinical Guideline',
                    'Clinical Performance Guideline', 'Medicare Advantage Coverage Summary']

policy_type_mapping = {"Reimbursement Policy":  "Payment Policy",
"Medical Policy": "Payment Policy",
"Medical Necessity": "Payment Policy",
"Clinical Policy": "Payment Policy",
"UnitedHealthcare Oxford Clinical and Administrative Policies":"Payment Policy",  
"Clinical and Administrative Policies":"Payment Policy",
"Administrative Policy":"Payment Policy",  
"Medical Benefit Drug Policy":"Drug Policy",    
"UnitedHealthcare West Benefit Interpretation Policy":"Payment Policy",    
"UnitedHealthcare West Medical Management Guideline":"Payment Policy",
"Coverage Determination Guideline":"Payment Policy",    
"Clinical Guidelines":"Clinical Guideline",
"Clinical Performance Guideline":"Clinical Guideline",
"Medicare Advantage Coverage Summary": "Payment Policy"
}

claim_type_list = [", Facility", ", Professional", "UB-04", "CMS-1500", ", Facility and Professional",
                   ", Professional and Facility", "CMS 1500", "UB 04"]


states_list = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
               "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
               "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
               "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
               "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
               "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
                'American Samoa','District of Columbia','Guam','North Mariana Islands','US Virgin Islands','Puerto Rico']


state_abbreviation_list = ['AL', 'AK', 'AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IA','KS','KY','LA','ME','MA','MI',
                           'MN','MS','MO','MT','NE','NV','NH','NJ','AS','DC','GU','IL','IN','MD','NM','NY','NC','ND','MP',
                           'OH','OK','OR','PA','PR','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','VI']

state_abbrev_mapping = {
    "Alabama":"AL", "Alaska":"AK", "Arizona":"AZ", "Arkansas":"AR", "California":"CA", "Colorado":"CO", "Connecticut":"CT", 
    "Delaware":"DE", "Florida":"FL", "Georgia":"GA", "Hawaii":"HI", "Idaho":"ID", "Illinois":"IL", "Indiana":"IN", "Iowa":"IA", 
    "Kansas":"KS", "Kentucky":"KY", "Louisiana":"LA", "Maine":"ME", "Maryland":"MD", "Massachusetts":"MA", "Michigan":"MI", "Minnesota":"MN", 
    "Mississippi":"MS", "Missouri":"MO", "Montana":"MT", "Nebraska":"NE","Nevada":"NV", "New Hampshire":"NH", "New Jersey":"NJ", 
    "New Mexico":"NM", "New York":"NY", "North Carolina":"NC", "North Dakota":"ND", "Ohio":"OH","Oklahoma":"OK", "Oregon":"OR", 
    "Pennsylvania":"PA", "Rhode Island":"RI", "South Carolina":"SC", "South Dakota":"SD", "Tennessee":"TN","Texas":"TX", "Utah":"UT", 
    "Vermont":"VT", "Virginia":"VA", "Washington":"WA", "West Virginia":"WV", "Wisconsin":"WI", "Wyoming":"WY",'American Samoa':"AS",
    'District of Columbia':"DC",'Guam':"GU",'North Mariana Islands':"MP",'US Virgin Islands':"VI",'Puerto Rico':"PR"
}

effective_date_patterns = ['Effective Date:','Effective Date','Effective ']

remove_list_policy_number =  ['PHARMACY', 'PAIN', 'SURGERY', 'ADMINISTRATIVE', 'TRANSPORT',
       'OUTPATIENT', 'DIAGNOSTIC', 'RESPIRATORY', 'CARDIOLOGY',
       'LABORATORY', 'EXPERIMENTAL', 'MATERNITY', 'REHABILITATION', 'ENT',
       'RADIOLOGY', 'DIABETIC', 'DME', 'CANCER', 'VISION', 'HOME',
       'DIALYSIS', 'REHAB', 'INFERTILITY', 'DERMATOLOGY', 'ANESTHESIA',
       'ALTERNATIVE', 'BEHAVIORAL', 'PREVENTIVE', 'DENTAL', "Policy Number ", "Policy Number: ","Annual Approval Date"]
remove_list_policy_number_new = ["Policy Number ", "Policy Number: ","Annual Approval Date"]
remove_word_title_list = ['Cs', 'Comm', 'Geha Coverage Policy', 'Vb', 'Exchange', 'Sv', 'Uhccp', 'Vb Exchange']

# line_of_business_dict = {
# ("exchange", "individual exchange medical policy") : 'Marketplace',
# ("exchange-reimbursement", "individual exchange reimbursement policy"): "Marketplace",
# ("exchange", "commercial reimbursement policy update bulletin") : 'Marketplace',
# ("exchange-reimbursement", "commercial reimbursement policy update bulletin") : 'Marketplace',
# ("comm-reimbursement", "commercial and individual exchange reimbursement policy"):['Commercial', 'Marketplace'],
# ("comm-medical-drug", "commercial and individual exchange"): ['Commercial', 'Marketplace'],
# ("comm-reimbursement", "commercial reimbursement policy"):'Commercial',
# ("comm-medical-drug", "unitedhealthcare commercial"): 'Commercial',
# ("comm-medical-drug", "commercial medical benefit drug policy"): 'Commercial',
# ("comm-reimbursement", "commercial reimbursement policy update bulletin"): 'Commercial',
# ("oxford", 'oxford clinical policy',): "Commercial",
# ("oxford", ' oxford administrative policy'): "Commercial",
# ("signaturevalue-bip", "west benefit interpretation policy"):"Commercial",
# ("signaturevalue-bip", " west benefit interpretation policy update bulletin"):"Commercial",
# ("signaturevalue-mmg", "west medical management guideline"):"Commercial",
# ("medicaid-comm-plan", "community plan medical benefit drug policy"):"Medicaid",
# ("medicaid-comm-plan", "community plan medical policy"):"Medicaid",
# ("medicaid-comm-plan","community plan coverage determination guideline"): 'Medicaid',
# ("medicaid-comm-plan-reimbursement", "reimbursement policy"):"Medicaid",
# ("medadv-coverage-sum", "medicare advantage coverage summary"):"Medicare Advantage",
# ("medadv-reimbursement", "medicare advantage reimbursement policy"):"Medicare Advantage",
# ("medadv-reimbursement", "medicare advantage reimbursement policy update bulletin"):"Medicare Advantage",
# ("medadv-guidelines", "medicare advantage policy guideline"):"Medicare Advantage",
# ("medical-policy-update-bulletin",):'Commercial',
# ('commercial',) : 'Commercial',
# ('medicare',):'Medicare'
# }

line_of_business_dict = {
    "exchange" : {
        "individual exchange medical policy": "Marketplace",
        "commercial reimbursement policy update bulletin": 'Marketplace'
    },
    "exchange-reimbursement" : {
        "individual exchange reimbursement policy": 'Marketplace',
        "commercial reimbursement policy update bulletin": 'Marketplace'
    },
    "comm-reimbursement":{
        "commercial and individual exchange reimbursement policy": ['Commercial', 'Marketplace'],
        "commercial reimbursement policy": "Commercial",
        "commercial reimbursement policy update bulletin":"Commercial"
    },
    "comm-medical-drug": {
        "commercial and individual exchange": ['Commercial', 'Marketplace'],
        "unitedhealthcare commercial":"Commercial",
        "commercial medical benefit drug policy": "Commercial"
    },
    "oxford": {
        "oxford clinical policy":"Commercial",
        "oxford administrative policy":"Commercial"
    },
    "signaturevalue-bip":{
        "west benefit interpretation policy":"Commercial",
        "west benefit interpretation policy update bulletin":"Commercial"
    },
    "signaturevalue-mmg":{
        "west medical management guideline":"Commercial"
    },
    "medicaid-comm-plan":{
        "community plan medical benefit drug policy":"Medicaid",
        "community plan medical policy":"Medicaid",
        "community plan coverage determination guideline":"Medicaid"
    },
    "medicaid-comm-plan-reimbursement":{
        "reimbursement policy":"Medicaid"
    },
    "medadv-coverage-sum":{
        "medicare advantage coverage summary":"Medicare Advantage"
    },
    "medadv-reimbursement":{
        "medicare advantage reimbursement policy":"Medicare Advantage",
        "medicare advantage reimbursement policy update bulletin":"Medicare Advantage"
    },
    "medadv-guidelines":{
        "medicare advantage policy guideline":"Medicare Advantage"
    },
    "medical-policy-update-bulletin":"Commercial",
    "commercial":"Commercial",
    "medicare":"Medicare"
}

uhc_service_level_dict = {('cms1500', 'professional') : 'Professional', ('cms1500', 'facility', 'professional') : 'Professional', ('cms1500', 'professional', 'ub04') : 'Professional',
                          ('cms1450', 'facility') : 'Facility', ('cms1450', 'facility', 'professional') : 'Facility', ('cms1450', 'cms1500', 'facility') : 'Facility', 
                          ('facility', 'ub04') : 'Facility', ('facility', 'professional', 'ub04') : 'Facility', ('cms1500', 'facility', 'ub04') : 'Facility', ('cms1450', 'facility', 'ub04') : 'Facility', 
                          ('cms1450', 'cms1500', 'professional') : 'Professional', ('cms1450', 'facility', 'professional', 'ub04') : 'Facility', ('cms1450', 'cms1500', 'facility', 'ub04') : 'Facility'
}

########### GROUP 1

def group1_icd10(matches):
    numbers=[]
    # matches = re.findall(r'([A-Za-z]{1})(\d{2}\.\d{2})-([A-Za-z]{1})(\d{2}\.\d{2})', ' L09.11-L10.23 ')
    for match in matches:
        start = Decimal(match[1])
        end = Decimal(match[3])
        if "." in str(start):
            int_length = Decimal(len(str(match[1]).split(".")[0]))
            decimal_number = Decimal(len(str(match[1]).split(".")[-1]))
            numbers.append(f'{str(match[0])}{num/10**(decimal_number):0{int_length+decimal_number+1}.{decimal_number}f}' for num in sorted(set(range(int(start*(10**(decimal_number))), int((end+(1/(10**(decimal_number))))*10**(decimal_number))))))
        else:
            int_length = Decimal(len(str(match[1])))
            numbers.append(f'{str(match[0])}{num:0{int_length}}' for num in sorted(set(range(int(start), int(end+1)))))
    return numbers
########### GROUP 2

def group2_icd10(matches):
    numbers=[]
    # matches = re.findall(r'([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{3})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{3})', ' C7A.021-C7A.029 abc')
    for match in matches:
        number_length = len(str(match[1]))
        start = int(match[1])
        end = int(match[3])
        numbers.append(f'{str(match[0])}{num:0{number_length}}' for num in (range(start, end+1)))
    return numbers
########### GROUP 3

def group3_icd10(matches):
    numbers=[]
    # matches = re.findall(r'([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{3})-([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{3})', ' anc S17.2XXA-S19.9XXA ')
    for match in matches:
        start = Decimal(match[1])
        end = Decimal(match[4])
        int_length = Decimal(len(str(match[1]).split(".")[0]))
        decimal_number = Decimal(len(str(match[1]).split(".")[-1]))
        numbers.append(f'{str(match[0])}{num/10**(decimal_number):0{int_length+decimal_number+1}.{decimal_number}f}{str(match[2])}' for num in sorted(set(range(int(start*(10**(decimal_number))), int((end+(1/(10**(decimal_number))))*10**(decimal_number))))))
    return numbers
########### GROUP 6 -   PATTERN 1

def group6_icd10(matches):
    numbers = []
    # matches = re.findall(r'([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})', "A C01.U-C02.B xsu")
    for match in matches:
        if str(match[0]) == str(match[3]):
            int_length = len(str(match[1]))
            for number in range(int(match[1]), int(match[4])+1):
                alpha1 = match[2] if (number == int(match[1])) else 'A'
                alpha2 = match[5] if ((number == int(match[1])) and (int(match[1]) == int(match[4]))) or (number == int(match[4])) else 'Z'
                for alphabet in range(ord(alpha1), ord(alpha2)+1):
                    numbers.append(f"{str(match[0])}{number:0{int_length}}.{chr(alphabet)}")
    return numbers
########### GROUP 4


def group4_icd10(matches):
    numbers = []
    # matches = re.findall(r'([A-Za-z]{1})(\d{2})\.(\d{2})([A-Za-z]{1})(\d{1})-([A-Za-z]{1})(\d{2})\.(\d{2})([A-Za-z]{1})(\d{1})', 'abc O31.06X2-O31.06X8')
    for match in matches:
        number1_length = len(str(match[2]))
        number2_length = len(str(match[4]))
        if len(str(match[2])) == 2:
            for number1 in range(int(match[2]), int(match[7])+1):
                range1 = int(match[4]) if number1 == int(match[2]) else 0
                range2 = int(match[9]) if ((number1 == int(match[2])) and (int(match[2]) == int(match[7]))) or (number1 == int(match[7])) else 9
                for number2 in range(range1, range2+1):
                    numbers.append(f'{str(match[0])}{str(match[1])}.{number1:0{number1_length}}{str(match[3])}{number2:0{number2_length}}')
        else:
            start = Decimal(match[4])
            end = Decimal(match[9])
            numbers.append(f'{str(match[0])}{str(match[1])}.{str(match[2])}{str(match[3])}{num:0{number2_length}}' for num in sorted(set(range(int(start), int(end)+1))))
    return numbers
########### GROUP 5 

def group5_icd10(matches):
    numbers = []
    # matches = re.findall(r'([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})([A-Za-z]{1})', ' abcd T09.A09A-T10.A11A')
    for match in matches:
        number1_length = len(str(match[1]))
        number2_length = len(str(match[3]))
        for number1 in range(int(match[1]), int(match[6])+1):
            range1 = int(match[3]) if number1 == int(match[1]) else 0
            range2 = 99 if ((number1 == int(match[1])) and (number2_length == 2) and (int(match[1])!=int(match[6]))) or ((number1 != int(match[1])) and (number1 != int(match[6])) and (number2_length == 2)) else 9 if ((number1 == int(match[1])) and (int(match[1])!=int(match[6]))) or ((number1 != int(match[1])) and (number1 != int(match[6]))) else int(match[8])
            for number2 in range(range1, range2+1):
                numbers.append(f'{str(match[0])}{number1:0{number1_length}}.{str(match[2])}{number2:0{number2_length}}{str(match[4])}')
    return numbers
########### GROUP 7
def group7_icd10(matches):
    numbers = []
    # matches = re.findall(r'([A-Za-z]{1}\d{2}\.[A-Za-z]{1})(\d{2})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1})(\d{2})', 'ab M01.X00-M01.X09 ch')
    for match in matches:
        number_length = len(str(match[1]))
        start = int(match[1])
        end = int(match[3])
        numbers.append(f'{str(match[0])}{num:0{number_length}}' for num in (range(start, end+1)))
    return numbers
###########GROUP 10 


def group10_icd10(matches):
    numbers = []
    # matches = re.findall(r'([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})([A-Za-z]{1})', 'abcd S72.8X2E-S72.8X2K xyz')
    for match in matches:
        number1_length = len(str(match[2]))
        number2_length = len(str(match[4]))
        if str(match[5]) == str(match[11]):
            for number1 in range(int(match[2]), int(match[8])+1):
                range1 = int(match[4]) if number1 == int(match[2]) else 0
                range2 = int(match[10]) if ((int(match[2]) == int(match[8])) and (number1 == int(match[2]))) or (number1 == int(match[8])) else 9
                for number2 in range(range1, range2+1):
                    numbers.append(f'{str(match[0])}{str(match[1])}.{number1:0{number1_length}}{str(match[3])}{number2:0{number2_length}}{str(match[5])}')
        elif str(match[5]) != str(match[11]):
            for number in range(int(match[4]), int(match[10])+1):
                alpha1 = match[5] if number == int(match[4]) else 'A'
                alpha2 = match[11] if ((int(match[4]) == int(match[10])) and (number == int(match[4]))) or (number == int(match[10])) else 'Z'
                for alphabet in range(ord(alpha1), ord(alpha2)+1):
                    numbers.append(f'{str(match[0])}{str(match[1])}.{str(match[2])}{str(match[3])}{number:0{number2_length}}{chr(alphabet)}')
    return numbers

icd10_patterns_dict = {
    r"([A-Za-z]{1})(\d{2})-([A-Za-z]{1})(\d{2})": group1_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{1})-([A-Za-z]{1})(\d{2}\.\d{1})": group1_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{2})-([A-Za-z]{1})(\d{2}\.\d{2})": group1_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{3})-([A-Za-z]{1})(\d{2}\.\d{3})": group1_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{4})-([A-Za-z]{1})(\d{2}\.\d{4})": group1_icd10,

    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{2})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{2})": group2_icd10,
    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{3})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{3})": group2_icd10,
    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{1})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.)(\d{1})": group2_icd10,

    r"([A-Za-z]{1})(\d{2}\.\d{2})([A-Za-z]{2})-([A-Za-z]{1})(\d{2}\.\d{2})([A-Za-z]{2})": group3_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{3})([A-Za-z]{1})-([A-Za-z]{1})(\d{2}\.\d{3})([A-Za-z]{1})": group3_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{1})-([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{1})": group3_icd10,
    r"([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{3})-([A-Za-z]{1})(\d{2}\.\d{1})([A-Za-z]{3})": group3_icd10,

    r"([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{2})-([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{2})": group4_icd10,
    r"([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{2})(\d{1})-([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{2})(\d{1})": group4_icd10,
    r"([A-Za-z]{1})(\d{2})\.(\d{2})([A-Za-z]{1})(\d{1})-([A-Za-z]{1})(\d{2})\.(\d{2})([A-Za-z]{1})(\d{1})": group4_icd10,
    r"([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})-([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})": group4_icd10,

    r"([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{1})([A-Za-z]{2})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{1})([A-Za-z]{2})": group5_icd10,
    r"([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})([A-Za-z]{1})": group5_icd10,

    r"([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})":group6_icd10,

    r"([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{1})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{1})":group7_icd10,
    r"([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})-([A-Za-z]{1})(\d{2})\.([A-Za-z]{1})(\d{2})":group7_icd10,

    r"([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})([A-Za-z]{1})-([A-Za-z]{1})(\d{2})\.(\d{1})([A-Za-z]{1})(\d{1})([A-Za-z]{1})":group10_icd10
}

code_range_patterns_list = [
    r"([A-Za-z]{1}\d{2})-([A-Za-z]{1}\d{2})",
    r"([A-Za-z]{1}\d{2}\.\d{1})-([A-Za-z]{1}\d{2}\.\d{1})",
    r"([A-Za-z]{1}\d{2}\.\d{2})-([A-Za-z]{1}\d{2}\.\d{2})",
    r"([A-Za-z]{1}\d{2}\.\d{3})-([A-Za-z]{1}\d{2}\.\d{3})",
    r"([A-Za-z]{1}\d{2}\.\d{4})-([A-Za-z]{1}\d{2}\.\d{4})",
    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{2})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{2})",
    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{3})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{3})",
    r"([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{1})-([A-Za-z]{1}\d{1}[A-Za-z]{1}\.\d{1})",
    r"([A-Za-z]{1}\d{2}\.\d{2}[A-Za-z]{2})-([A-Za-z]{1}\d{2}\.\d{2}[A-Za-z]{2})",
    r"([A-Za-z]{1}\d{2}\.\d{3}[A-Za-z]{1})-([A-Za-z]{1}\d{2}\.\d{3}[A-Za-z]{1})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{3})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{3})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{2})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{2})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{2}\d{1})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{2}\d{1})",
    r"([A-Za-z]{1}\d{2}\.\d{2}[A-Za-z]{1}\d{1})-([A-Za-z]{1}\d{2}\.\d{2}[A-Za-z]{1}\d{1})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{1})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{1})",
    r"([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{1}[A-Za-z]{2})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{1}[A-Za-z]{2})",
    r"([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{2}[A-Za-z]{1})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{2}[A-Za-z]{1})",
    r"([A-Za-z]{1}\d{2}\.[A-Za-z]{1})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1})",
    r"([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{1})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{1})",
    r"([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{2})-([A-Za-z]{1}\d{2}\.[A-Za-z]{1}\d{2})",
    r"([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{1}[A-Za-z]{1})-([A-Za-z]{1}\d{2}\.\d{1}[A-Za-z]{1}\d{1}[A-Za-z]{1})",
    r"(?<![^\s])(\d+)-(\d+)(?![^\s])",
    r"(\d+[A-Za-z]{1})-(\d+[A-Za-z]{1})",
    r"([A-Za-z]{1}\d+)-([A-Za-z]{1}\d+)"
]

def find_closest_code(codes_list, target_code, range_element):
    closest_code = None
    min_difference = float('inf')
    for code in codes_list:
        if len(code) == len(target_code):
            if ((target_code[0].isalpha()) and (code[0] == target_code[0])) or (target_code[-1].isalpha()): 
                difference = sum(c1 != c2 for c1, c2 in zip(target_code, code))
                if difference < min_difference:
                    closest_code = code
                    min_difference = difference
    if (closest_code == None) and (target_code.isnumeric()):
        if range_element == 'start':
            print('start')
            closest_code = str(int(target_code) - 1)
            while (closest_code not in codes_list) and (int(closest_code)>0):
                closest_code = str(int(closest_code) - 1)
        elif range_element == 'end':
            print('end')
            closest_code = str(int(target_code) + 1)
            while (closest_code not in codes_list) and (int(closest_code)<99999):
                closest_code = str(int(closest_code) + 1)
    print(f"The closest code to {target_code} in the list is {closest_code}")
    print("The index of the closest code is: ", codes_list.index(closest_code))
    return closest_code

def codes_between_range(ref_code_list, start_value, end_value):
    numbers_list = []
    if ref_code_list.index(start_value) <= ref_code_list.index(end_value):
        print("indices are:: ",ref_code_list.index(start_value), ref_code_list.index(end_value))
        numbers_list = ref_code_list[ref_code_list.index(start_value):ref_code_list.index(end_value)+1]
    else:
        print("indices are:: ",ref_code_list.index(end_value), ref_code_list.index(start_value))
        numbers_list = ref_code_list[ref_code_list.index(end_value):ref_code_list.index(start_value)+1]
    return numbers_list

def read_csv_from_s3(object_key):
    try:
        response = s3_client.get_object(Bucket='zai-reference-data', Key=object_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()), dtype=str)
        df.fillna('NA', inplace=True)
        return df
    except Exception as e:
        print("Error: ", e)
        return None

reference_code_df = read_csv_from_s3('paymentPoliciesReference/reference_code_table.csv')

def code_range(matches):
    sorted_icd10_df = reference_code_df[reference_code_df['code_type'].isin(['CPT', 'HCPCS', 'ICD10'])].sort_values(by=['code_type', 'code'], ascending=True)[['code']].reset_index(drop=True)
    sorted_codelist = sorted_icd10_df['code'].to_list()
    all_codes=[]
    for match in matches:
        try:
            start = match[0]
            end = match[1]
            # print(start, end)
            if (start in sorted_codelist) and (end in sorted_codelist):
                print('both numbers in ref sheet', start, end)
                all_codes.extend(codes_between_range(sorted_codelist, start, end))
            elif ((start not in sorted_codelist) and (end in sorted_codelist)):
                start = find_closest_code(sorted_codelist, start, 'start')
                all_codes.extend(codes_between_range(sorted_codelist, start, end))
            elif ((end not in sorted_codelist) and (start in sorted_codelist)):
                end = find_closest_code(sorted_codelist, end, 'end')
                all_codes.extend(codes_between_range(sorted_codelist, start, end))
            else:
                print("Both the elements of the range are not in the reference code table indicating it is less likely to be a code range.")
        except Exception as e:
            print("Error:- ", e)
    return all_codes

def read_pdf_from_s3(bucket, object_key):
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=object_key)
        pdf_byte = resp['Body'].read()
        text = extract_text(BytesIO(pdf_byte))
        return pdf_byte, text
    except Exception as e:
        print("Error: ", e)
        text = ''
        pdf_byte = ''
        return pdf_byte, text
    

def payer_name_hard_match(string, text): 
    if re.search(string,text):
        payer_name = re.search(string, text).group()
    else:
        payer_name = 'NA'
    return payer_name


def claimType(text):
    
    claim_type = re.findall(r'|'.join(claim_type_list), text)
    claim_type = list(set(claim_type))

    if len(claim_type) >= 1:
        for i in claim_type:
            if 'and' in i:
                return 'CMS 1500 & UB-04'
            elif 'UB' or 'Facility' in i:
                return 'UB-04'
            elif 'CMS' or 'Professional' in i:
                return 'CMS 1500'
            else:
                return 'NA'
    else: 
        return 'NA'

def policyType(pdf_link, text):
    d = {}
    mapped_policy = 'NA'
    try:
        for title in ['bulletin', 'rpub', 'mpub']:
            if title in [pdf_element.lower() for pdf_element in pdf_link.split("/")[-1].split(".")[0].split("-")]:
                mapped_policy = "Bulletins & Updates"
    except:
        if 'clinical-guidelines' in [link_element.lower() for link_element in pdf_link.split('/')]:
            mapped_policy = "Clinical Guideline"
    if mapped_policy == 'NA':
        if (pdf_link.split("/")[2]).lower() == 'www.rmhp.org':
            mapped_policy = "Payment Policy"
        else:
            lines = text.split("\n")
            for line in lines:
                res = re.findall(r"Policy type: ", line)
                if res:
                    policy = re.split(r"Policy type: ", line)[-1].strip()
                    mapped_policy = policy_type_mapping.get(policy, policy)
                    break
    if mapped_policy == 'NA':
        initial_para = "".join(text.split("\n")[:10])
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s+]', '', initial_para)
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
        for text_batch in [cleaned_text, text]:
            d = {policy_type: len(re.findall(policy_type, text_batch)) for policy_type in policy_type_list}
            sorted_d = sorted(d.items(), key=lambda item: item[1], reverse=True)
            if sorted_d[0][1] != 0:
                break    
        if sorted_d[0][1] != 0:    
            policy = sorted_d[0][0]
        else:
            policy = "Medical Policy"
        mapped_policy = policy_type_mapping.get(policy, policy)
    return mapped_policy

def policy_number_regex_match(value):

    pattern = ''
    digit_count = 0
    alpha_count = 0

    for i in value:
        if i.isdigit():
            digit_count += 1
            if alpha_count > 0:
                pattern += '[A-Za-z]{' + str(alpha_count) + '}'
                alpha_count = 0
        elif i.isalpha():
            alpha_count += 1
            if digit_count > 0:
                pattern += r'\d{' + str(digit_count) + '}'
                digit_count = 0
        else:
            if alpha_count > 0:
                pattern += '[A-Za-z]{' + str(alpha_count) + '}'
                alpha_count = 0
            elif digit_count > 0:
                pattern += r'\d{' + str(digit_count) + '}'
                digit_count = 0
            pattern += re.escape(i)

    if digit_count > 0:
        pattern += r'\d{' + str(digit_count) + '}'
    elif alpha_count > 0:
        pattern += '[A-Za-z]{' + str(alpha_count) + '}'

    return pattern


def extract_policy_number(key, value, text):

    lines = text.split("\n")
    pattern = policy_number_regex_match(value)
    string = key + " " + pattern
    match = re.search(string, text)
    if match:
        policy_number = match.group()
    else:
        for line in lines:
            res = re.findall(r"AUTH:|ACG:|Policy Number |Policy Number: |Policy Number|Guideline Number: |Guideline Number |Policy # |Number: |Number ", line)
            if res:
                policy_number = re.split(r"AUTH:|ACG:|Policy Number |Policy Number: |Policy Number|Guideline Number: |Guideline Number |Policy # |Number: |Number ", line)
                policy_number = policy_number[-1].strip()
                break
            else:
                policy_number = 'NA'
                
    for word in remove_list_policy_number_new:
        if word in policy_number:
            policy_number = policy_number.replace(word, "").strip()
    return policy_number

def effective_date_regex(text, word):
    effective_date = re.findall(rf"{word}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,2}}[-./, _:|]\d{{2,4}}", text, re.IGNORECASE) or re.findall(rf"{word}(?::)?\s+\d{{1,4}}[-./, _:|]\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}(?:st|nd|rd|th)?", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\d{{1,4}}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?\s+\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}(?:st|nd|rd|th)?,\s+\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"\b{word}(?::)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}},\s*?\d{{2,4}}\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{1,4}}", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{{1,2}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{{4}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}})\b", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text, re.IGNORECASE) or re.findall(
        rf"{word}(?::)?\s+\b\d{{1,4}},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text, re.IGNORECASE) or re.findall(rf"{word}(?::)?\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{{1,4}}", text, re.IGNORECASE)
    if effective_date:
        return effective_date[0]
    else:
        return 'NA'


def dates_from_text(text1):

    text2 = re.sub(r"[A-Za-z0-9][ ]\d{1,4}[-./, _:|]\d{1,2}[-./, _:|]\d{2,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9][ ]\d{1,4}[-./, _:|]\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9][ ]\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9][ ]\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{1,4}", "False positive_date", text1) or re.sub(
        r"[A-Za-z0-9][ ]\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", "False positive_date", text1) or re.sub(r"[A-Za-z0-9][ ]\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9][ ]\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", text1)
    a = re.findall(r"[A-Za-z0-9][ ]\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{2,4}", text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}[-./\, _:|]\d{1,4}", text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?",  text1) or re.findall(r"\b[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?",  text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}",  text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b",  text1) or re.findall(
        r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", text1) or re.findall(r"\b[A-Za-z0-9][ ]((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b",  text1) or re.findall(r"\b[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)",  text1) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}",  text1)
    res = [i.split(" ")[-1] for i in a]
    return text2


def check_effective_date_history(text, section_name):
    effective_date = 'NA'
    lines = text.split("\n")
    df = section_cleanup(text)
    df3 = section_range(df)
    if len(df3) != 0:
        if section_name in df3['section'].to_list():
            section_text = extract_text_for_section(df3, section_name, lines)
            sec_text_joined = "\n".join(section_text)
            if section_name != 'Policy History':
                fp_dates_ex_text = dates_from_text(sec_text_joined)
            else:
                fp_dates_ex_text = sec_text_joined
            dates = re.findall(r"(?<!\d)\d{4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{1,2}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\d{1,2}[-./\, _:|]\d{1,2}[-./\, _:|]\d{4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\d{1,4}[-./\, _:|]\d{1,4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b(?!\d)",
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    fp_dates_ex_text) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?!\d)", fp_dates_ex_text) or re.findall(r"(?<!\d)\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b(?!\d)", fp_dates_ex_text)
            date_objects = []
            for date in dates:
                for pattern, date_format in date_formats.items():
                    if re.match(pattern, date):
                        date_objects.append(datetime.strptime(date, date_format))
                        effective_date = max(date_objects).strftime(date_format)
                        oldest_date = min(date_objects).strftime(date_format)
                        break
    return effective_date


effective = []
def check_effective_date(text):
    line_text = " ".join((text.lower()).split("\n")[:35])
    effective_date = 'NA'
    for word in ['effective date', 'effective', 'approval date', 'implementation date', 'implementation', 'approved']:
        if word in line_text.lower().strip():
            effective_date1 = effective_date_regex(line_text, word)
            if effective_date1 == 'NA':
                continue
            else:
                effective_date = effective_date1.split(
                effective_date1.split(" ")[0])[-1].replace('Date:', '').replace('Date ', '').replace('Date', '').replace('date:', '').replace('date ', '').replace('date', '').replace(':', '').strip()
                break
    return effective_date


def get_sections(text):
    lines = text.split("\n")
    final_lines = [re.sub(r"[.]+\s+[0-9]+", "", line) for line in lines]
    section = set()
    for n, line in enumerate(final_lines):
        line = re.sub(r"\s+", " ", line)
        line = line.strip()
        for sec in sections_list:
            if line.startswith(sec) and len(line.strip()) == len(sec):
                if section_mapper.get(sec):
                    sec = section_mapper.get(sec)
                section.add((n, sec))
    return section


def section_cleanup(text):

    lines = text.split("\n")
    section = get_sections(text)
    tuples = sorted(section, key=lambda i: i[0])
    d = {}
    second_time_word = None
    for i, word in tuples:
        if word in history_section_list:
            d[word] = d.get(word, 0) + 1

            if d[word] == 2:
                second_time_word = i

    if second_time_word is not None:

        new_section = []
        for i, word in tuples:
            if i > second_time_word:
                if word not in remove_list:
                    new_section.append((i, word))
            else:
                new_section.append((i, word))
    else:
        new_section = tuples
    section_df = pd.DataFrame(new_section, columns=["start", "section"])
    section_df["end"] = section_df["start"].shift(-1) - 1
    if len(section_df) !=0:
        section_df.at[section_df.index[-1], "end"] = section_df.at[section_df.index[-1],
                                                                "start"] + len(lines)
        section_df["end"] = section_df["end"].astype(int)
    return section_df


def section_range(df):

    df["difference"] = df["end"]-df["start"]
    df2 = df.groupby("section")["difference"].agg(
        "max").sort_values(ascending=True).reset_index()

    tuple_list = list(zip(df2["section"], df2["difference"]))

    new_rows = []
    for section, difference in tuple_list:
        filtered_rows = df[(df["section"] == section) &
                           (df["difference"] == difference)]

        for i, row in filtered_rows.iterrows():
            new_rows.append(
                (row['start'], section, difference, row['start'] + difference))

    df3 = pd.DataFrame(new_rows, columns=[
                       'start', 'section', 'difference', 'end'])
    return df3


def extract_text_for_section(df3, section_name, lines):
    row = df3[df3["section"] == section_name]
    if len(row) == 0:
        return "NA"
    else:
        last_section = row.index[-1] == df3.index[-1]
        start = row.iloc[0]["start"]
        end = row.iloc[0]["end"]
        if last_section:
            section_text = lines[start:]
        else:
            section_text = lines[start:end+1]
        return section_text

def remove_section_text(df3, section_name, lines):
    row = df3[df3["section"] == section_name]
    if len(row) == 0:
        return "NA"
    else:
        last_section = row.index[-1] == df3.index[-1]
        start = row.iloc[0]["start"]
        end = row.iloc[0]["end"]
        if last_section:
            section_text = lines[:start]
        else:
            section_text1 = lines[:start]
            section_text2 = lines[end+1:]
            section_text = section_text1 + section_text2
        return section_text

def codes_extraction_with_type(lines):
    result_list = []
    raw_codes = []
    for line in lines:
        pattern_cpt = r'(?<![A-Za-z])(\b\d{4}[A-Z]\b|\b[A-Z]\d{2,4}\b|\b[A-Z]\d{1,3}.\d{1,4}\b|\b[A-Za-z]{1}\d{2}.[A-Za-z]{1}\d{1,2}\b|\b[A-Za-z]{1}\d{2}.\d{1,3}[A-Za-z]{1,3}\b|\b[A-Za-z]{1}\d{2}.[A-Za-z]{1,2}\b|\b[A-Za-z]{1}\d{2}.[A-Za-z]{1}\d{1,2}[A-Za-z]{1,2}\b|\b[A-Za-z]{1}\d{2}.\d{1}[A-Za-z]{1}\d{1}[A-Za-z]{1}\b|\b[A-Za-z]{1}\d{2}.\d{1}[A-Za-z]{1}\b|\b[A-Za-z]{1}\d{1}[A-Za-z]{1}\b|\b[A-Za-z]{2}\d{3}[A-Za-z]{1}\d{1}\b|\b[A-Z]\d{1,3}.\d{1,2}[A-Z]{1,2}\d{1,2}\b|\b[A-Z]\d{1,3}[A-Z].\d{1,3}\b|\b\d{3,5}\b)(?![-.])'
        res = re.findall(pattern_cpt, line)
        if res:
            raw_codes.extend(res)
            raw_codes = list(set(raw_codes))
        else:
            continue
    for item in raw_codes:
        if item in reference_code_df[reference_code_df['code_type'].isin(['CPT', 'HCPCS', 'ICD10'])]['code'].values:
            result_list.append(item)
    return result_list

pattern_dict = {r"([A-Za-z]+)(\d+)-([A-Za-z]+)(\d+)": [0,1,3],
                r"([A-Za-z]+)(\d+)-(\d+)": [0,1,2],
                r"(\d+)([A-Za-z]+)-(\d+)([A-Za-z]+)": [1,0,2],
                r"(\d+)-(\d+)([A-Za-z]+)": [2,0,1],
                r"\b(\d+)-(\d+)\b": ["",0,1]
            }

def code_range_extraction(lines):
    l1 = []
    result_list = []
    for line in lines:
        # print("LINE OF TEXT: ", line)
        for pattern in pattern_dict.keys():
            # print(pattern)
            matches = re.findall(pattern, line)
            # print(matches)
            if matches:
                for match in matches:
                    prefix_index_value, start_num_index_value, end_num_index_value = pattern_dict[pattern]
                    # print(prefix_index_value, start_num_index_value, end_num_index_value)
                    if prefix_index_value != "":
                        prefix = match[prefix_index_value]
                    start, end = match[start_num_index_value], match[end_num_index_value]
                    numbers = list(range(int(start), int(end) + 1))
                    for num in numbers:
                        if prefix_index_value == 0:
                            l1.append(prefix + f"{num:0{len(start)}}")
                        elif (prefix_index_value == 1)|(prefix_index_value == 2):
                            l1.append(f"{num:0{len(start)}}" + prefix)
                        elif prefix_index_value == "":
                            l1.append(f"{num:0{len(start)}}")
    for item in l1:
        if item in reference_code_df['code'].values:
            # code_type = reference_code_df[reference_code_df['code'] == item]['code_type'].values[0]
            result_list.append(item)
    return result_list

def state_exceptions(section_text):

    states = []
    for state in states_list:
        for line in section_text:
            if state in line:
                states.append(state)
                states = list(set(states))
            else:
                continue
    return states

# def extract_hyperlinks(bucket, object_key):
#     try:
#         pdf_byte, text = read_pdf_from_s3(bucket, object_key)
#         # file = s3_client.get_object(Bucket=bucket, Key=object_key)
#         pdf_file = pikepdf.Pdf.open(BytesIO(pdf_byte))
#         urls = []
#         for page in pdf_file.pages:
#             if page.get("/Annots"):
#                 for annots in page.get("/Annots"):
#                     if annots.get("/A"):
#                         url=annots.get("/A").get("/URI")
#                         if (url is not None) and (url.endswith('.pdf')): # and ('www.uhcprovider.com' in [element.lower() for element in url.split("/")]):
#                             urls.append(str(url))
#         if len(urls) == 0:
#             urls.append("NA")
#         return urls
#     except Exception as e:
#         print("Error  :- ",e)
#         return None

def extract_hyperlinks(bucket, object_key):
    try:
        print(f"bucket and object key are: {bucket}, {object_key}")
        s3_object = s3_client.get_object(Bucket=bucket, Key=object_key)
        file_stream = BytesIO(s3_object['Body'].read())
 
        with pikepdf.open(file_stream) as pdf:
            print(f"Number of pages: {len(pdf.pages)}")
            urls = []
            for page in pdf.pages:
                if page.get("/Annots", None):
                    for annots in page.get("/Annots", None):
                        if annots.get("/A", None):
                            url=annots.get("/A", None).get("/URI", None)
                            if (str(url) is not None) and (str(url).endswith('.pdf')) and ('www.uhcprovider.com' in [element.lower() for element in str(url).split("/")]):
                                urls.append(str(url))
            if len(urls) == 0:
                urls.append("NA")
            return urls
    except Exception as e:
        print("Error  :- ",e)
        return None

def nt_check_effective_date(text):
    effective_date = 'NA'
    lines = text.split("\n")
    split_text = " ".join(text.split("\n")[:40])
    indexes = []
    for word in ['effective date', 'effective', 'approval date', 'implementation date', 'implementation', 'approved']:
        if word in text.lower().strip():
            effective_date1 = effective_date_regex(text, word)
            if effective_date1 == 'NA':
                continue
            else:
                effective_date = effective_date1.split(
                effective_date1.split(" ")[0])[-1].replace('Date:', '').replace('Date ', '').replace('Date', '').replace('date:', '').replace('date ', '').replace('date', '').replace(':', '').strip()
                break
    if effective_date == 'NA':
        for index, line in enumerate(lines):
            if ("History" in line and len(line.strip()) <= len("History")) or ("Policy History/Revision Information" in line and len(line.strip()) <= len("Policy History/Revision Information")) or ("Guidline History/Revision Information" in line and len(line.strip()) <= len("Guidline History/Revision Information")) or ("Policy History" in line and len(line.strip()) <= len("Policy History")):
                indexes.append(index)
    if len(indexes) > 0:
        lines1 = lines[indexes[-1]:]
        split_text = "\n".join(lines1)
        if "Policy History" in line and len(line.strip()) <= len("Policy History"):
            date_text = dates_from_text(split_text)
        else:
            date_text = split_text
        dates = re.findall(r"(?<!\d)\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{1,2}(?!\d)", date_text) or re.findall(r"\d{1,4}[-./\, _:|]\d{1,4}", date_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?", date_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?", date_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", date_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", date_text)  or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", date_text) or re.findall(r"\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", date_text)  or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", date_text) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", date_text)  or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b", date_text)
        date_objects = []
        for date in dates:
            for pattern, date_format in date_formats.items():
                if re.match(pattern, date):
                    date_objects.append(datetime.strptime(date, date_format))
                    effective_date = max(date_objects).strftime(date_format)
                    oldest_date = min(date_objects).strftime(date_format)
                    break
    return effective_date