import pandas as pd
import re
from datetime import datetime
from constants import date_formats,s3_client
import io
import pikepdf 
from pdfminer.high_level import extract_text
from io import BytesIO

sections_list = ["Table of Contents", "Application", "Reimbursement Guidelines", "Definitions", "Anatomic Modifiers",
                 "Questions and Answers", "Codes", "Resources", "Attachments", "Attachments: Please right click on the icon to open the file",
                 "State Exceptions", "Coverage Rationale", "Applicable Codes", "Policy","Exceptions", "To Review Guidelines","Description",
                 "References", "Instructions for Use", "Description of Services", "Clinical Evidence",
                 "U.S. Food and Drug Administration (FDA)", "Background", "Benefit Considerations",
                 "Centers for Medicare & Medicaid Services (CMS)", "Coverage Guidlines", "Documentation Requirements",
                 "History", "Policy History/Revision Information", "Guidline History/Revision Information", 'Policy approval and history:'
                 ]

history_section_list = ["History", "Policy History/Revision Information", "Guidline History/Revision Information"]


remove_list = ["Table of Contents", "Application", "Reimbursement Guidelines", "Definitions", "Anatomic Modifiers",
               "Questions and Answers", "Codes", "Resources", "History", "Attachments", "Attachments: Please right click on the icon to open the file",
               "State Exceptions", "Coverage Rationale", "Applicable Codes", "History","Exceptions", "To Review Guidelines","Description",
               "Policy History/Revision Information", "References", "Guidline History/Revision Information",
               "Description of Services", "Clinical Evidence",
               "U.S. Food and Drug Administration (FDA)", "Background", "Benefit Considerations",
               "Centers for Medicare & Medicaid Services (CMS)", "Coverage Guidlines", "Documentation Requirements"
               ]

sub_section_list = ["Reimbursement Guidelines"]


section_mapper = {"Attachments: Please right click on the icon to open the file": "Attachments"}

policy_type_list = ["Reimbursement Policy", "Medical Policy", "Drug Policy", "Clinical Policy",
                    "United Healthcare Oxford Clinical and Administrative Policies",
                    "Clinical and Administrative Policies", "Administrative Policy",
                    "Medical Benefit Drug Policy", "United Healthcare West Benefit Interpretation Policy",
                    "United Healthcare West Medical Management Guideline", 
                    "Coverage Determination Guideline","Clinical Guidelines",'Clinical Guideline',
                    'Clinical Performance Guideline']

claim_type_list = [", Facility", ", Professional", "UB-04", "CMS-1500", ", Facility and Professional",
                   ", Professional and Facility", "CMS 1500", "UB 04"]


states_list = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
               "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
               "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
               "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
               "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
               "Texas", "Utah", "Vermont", "Virginia", "Washington", "Washington DC" "West Virginia", "Wisconsin", "Wyoming"]

effective_date_patterns = ['Effective Date:','Effective Date','Effective ']

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
# reference_code_df = pd.read_csv(r"C:\Users\User\Downloads\test_abstraction_010124\reference_code_table.csv",dtype = str)


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

def policyType(text):
    
    d = {}
    for policy_type in policy_type_list:
        result = re.findall(policy_type, text)
        d[policy_type] = len(result)
        new_d = sorted(d.items(), key=lambda item: item[1], reverse=True)
        policy = new_d[0][0]

    return policy


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
            res = re.findall(r"Policy Number |Policy Number: ", line)
            if res:
                policy_number = re.split(r"Policy Number |Policy Number: ", line)
                policy_number = policy_number[-1].strip()
                break
            else:
                policy_number = 'NA'

    return policy_number

def effective_date_regex(text):

    effective_date = re.findall(r"Effective Date(?::)?\s+\d{1,4}[-./, _:|]\d{1,2}[-./, _:|]\d{2,4}", text) or re.findall(r"Effective Date:\s+\d{1,4}[-./, _:|]\d{1,4}", text) or re.findall(r"Effective Date:\s+\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?", text) or re.findall(r"Effective Date:\s+\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?", text) or re.findall(r"Effective Date:\s+\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", text) or re.findall(r"Effective Date:\s+\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", text) or re.findall(r"Effective Date:\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", text) or re.findall(r"Effective Date:\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b", text) or re.findall(
        r"Effective Date:\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", text) or re.findall(r"Effective Date:\s+\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", text) or re.findall(r"Effective Date:\s+\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text) or re.findall(r"Effective Date:\s+\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", text) or re.findall(r"Effective Date:\s+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", text) or re.findall(r"\bEffective Date: (?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b", text, re.IGNORECASE)
    if effective_date:
        return effective_date[0]


def dates_from_text(text1):

    text2 = re.sub(r"[A-Za-z0-9]+[ ]+\d{1,4}[-./, _:|]\d{1,2}[-./, _:|]\d{2,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9]+[ ]+\d{1,4}[-./, _:|]\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9]+[ ]+\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9]+[ ]+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{1,4}", "False positive_date", text1) or re.sub(
        r"[A-Za-z0-9]+[ ]+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", "False positive_date", text1) or re.sub(r"[A-Za-z0-9]+[ ]+\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", "False positive_date", text1) or re.sub(r"[A-Za-z0-9]+[ ]+\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", text1)
    a = re.findall(r"[A-Za-z0-9][ ]\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{2,4}", text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}[-./\, _:|]\d{1,4}", text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?",  text1) or re.findall(r"\b[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?",  text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}",  text1) or re.findall(r"[A-Za-z0-9][ ]\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b",  text1) or re.findall(
        r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", text1) or re.findall(r"\b[A-Za-z0-9][ ]((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b",  text1) or re.findall(r"\b[A-Za-z0-9][ ]\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)",  text1) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)",  text1) or re.findall(r"\b[A-Za-z0-9][ ](?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}",  text1)
    res = [i.split(" ")[-1] for i in a]
    return text2


def check_effective_date_history(text, section_name):

    lines = text.split("\n")
    df = section_cleanup(text)
    df3 = section_range(df)
    if len(df3) != 0:
        if section_name in df3['section'].to_list():
            section_text = extract_text_for_section(df3, section_name, lines)
            sec_text_joined = "\n".join(section_text)
            fp_dates_ex_text = dates_from_text(sec_text_joined)
            dates = re.findall(r"\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{2,4}", fp_dates_ex_text) or re.findall(r"\d{1,4}[-./\, _:|]\d{1,4}", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?", fp_dates_ex_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b",
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", fp_dates_ex_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", fp_dates_ex_text) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", fp_dates_ex_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b", fp_dates_ex_text)
            date_objects = []
            for date in dates:
                for pattern, date_format in date_formats.items():
                    if re.match(pattern, date):
                        date_objects.append(datetime.strptime(date, date_format))
                        effective_date = max(date_objects).strftime(date_format)
                        oldest_date = min(date_objects).strftime(date_format)
                        break
                    else:
                        effective_date = None
        else:
            effective_date = nt_check_effective_date(text)
    else:
        effective_date = 'NA'
    return effective_date


effective = []
def check_effective_date(text):

    line_text = " ".join(text.split("\n")[:35])
    effective_date = 'NA'
    if "effective date" in line_text.lower().strip() or "effective" in line_text.lower().strip():
        effective_date1 = effective_date_regex(line_text)
        if effective_date1:
            effective_date = effective_date1.split(
                "Effective")[-1].replace('Date:', '').replace('Date ', '').replace('Date', '').replace(':', '').strip()
    else:
        effective_date = nt_check_effective_date(text)
        if effective_date is None:
            effective_date = 'NA'
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

def codes_extraction_with_type(lines):

    result_list = []
    raw_codes = []
    for line in lines:
        pattern_cpt = r'(?<![A-Za-z]{2} )(\b\d{4}[A-Z]\b|\b[A-Z]\d{4}\b|\b[A-Z]\d{1,3}.\d{1,3}\b|\b[A-Z]\d{1,3}.\d{1,2}[A-Z]\d{1,2}\b|\b[A-Z]\d{1,3}[A-Z].\d{1,3}\b|\b\d{5}\b(?![-.]))'
        res = re.findall(pattern_cpt, line)
        if res:
            raw_codes.extend(res)
            raw_codes = list(set(raw_codes))
        else:
            continue
    for item in raw_codes:
        if item in reference_code_df['code'].values:
            # code_type = reference_code_df[reference_code_df['code'] == item]['code_type'].values[0]
            result_list.append(item)
    # if len(result_list)==0:
    #     result_list.append(("NA","NA"))
    return result_list

pattern_dict = {r"([A-Za-z]+)(\d+)-([A-Za-z]+)(\d+)": [0,1,3],
                r"([A-Za-z]+)(\d+)-(\d+)": [0,1,2],
                r"(\d+)([A-Za-z]+)-(\d+)([A-Za-z]+)": [1,0,2],
                r"(\d+)-(\d+)([A-Za-z]+)": [2,0,1],
                r"\b(\d+)-(\d+)\b": ["",0,1]
                # r"([A-Za-z]+)(^\d+\.\d+$)-([A-Za-z]+)(^\d+\.\d+$)" :[0,1,3]
            }

def code_range_extraction(lines):
    l1 = []
    result_list = []
    for line in lines:
        for pattern in pattern_dict.keys():
            matches = re.findall(pattern, line)
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
            if state in line and len(line.strip()) <= len(state):
                states.append(state)
            else:
                continue
    return states

def extract_hyperlinks(bucket, object_key):

    try:
        pdf_byte, text = read_pdf_from_s3(bucket, object_key)
        pdf_file = pikepdf.Pdf.open(BytesIO(pdf_byte)) 
    
        urls = []
        for page in pdf_file.pages:
            if page.get("/Annots"):
                for annots in page.get("/Annots"):
                    if annots.get("/A"):
                        url=annots.get("/A").get("/URI")
                        if url is not None:
                            urls.append(str(url))
        if len(urls) == 0:
            urls.append("NA")
        return urls
    except Exception as e:
        print("Error: ",e)
        return None

def nt_check_effective_date(text):

    lines = text.split("\n")
    split_text = " ".join(text.split("\n")[:40])
    indexes = []
    
    if "Effective Date:" in split_text or "Effective" in split_text:
        effective_date1 = effective_date_regex(split_text)
        effective_date = effective_date1.split(":")[-1].strip()
    else:
        for index, line in enumerate(lines):
            if ("History" in line and len(line.strip()) <= len("History")) or ("Policy History/Revision Information" in line and len(line.strip()) <= len("Policy History/Revision Information")) or ("Guidline History/Revision Information" in line and len(line.strip()) <= len("Guidline History/Revision Information")):
                indexes.append(index)
    if len(indexes) > 0:
        lines1 = lines[indexes[-1]:]
        split_text = "\n".join(lines1)
        date_text = dates_from_text(split_text)
        dates = re.findall(r"\d{1,4}[-./\, _:|]\d{1,2}[-./\, _:|]\d{2,4}", date_text) or re.findall(r"\d{1,4}[-./\, _:|]\d{1,4}", date_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?", date_text) or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}(?:st|nd|rd|th)?", date_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", date_text) or re.findall(r"\d{1,4}(?:st|nd|rd|th)?\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", date_text)  or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?\s+\d{2,4}\b", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}(?:st|nd|rd|th)?,\s+\d{2,4}\b", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4}", date_text) or re.findall(r"\b((?:Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thur|Friday|Fri|Saturday|Sat|Sunday|Sun),\s+\d{1,2}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\b", date_text)  or re.findall(r"\b\d{1,4}\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", date_text) or re.findall(r"\b\d{1,4},\s+(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)", date_text)  or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December),\s+\d{1,4}", date_text) or re.findall(r"\b(?:January|Jan|Feb|February|Mar|March|April|May|June|July|Aug|August|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,4},\s+\d{2,4}\b", date_text)

        date_objects = []
        for date in dates:
            for pattern, date_format in date_formats.items():
                if re.match(pattern, date):
                    date_objects.append(datetime.strptime(date, date_format))
                    effective_date = max(date_objects).strftime(date_format)
                    oldest_date = min(date_objects).strftime(date_format)
                    break
        return effective_date
    return "NA"