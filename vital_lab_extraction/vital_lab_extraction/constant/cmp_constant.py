# variables
chem_lab_test_master_list = ['glucose', 'carbone dioxide', 'sodium', 'potassium', 'calcium', 'bun', 'creatinine', 'bilirubin', 'magnesium', 'phosphate',
                             'blood sugar', 'bs', 'co2', 'na', 'mg', 'po4', 'ca', 'phos', 'glu', 'chloride']
chem_sub_section_list = [
    'basic metabolic panel nonfasting 12292020 114', 'basic metabolic panel 02012020 417 cst', 'basic metabolic panel 12282020 213',
    'basic metabolic panel fasting 01092021 1256', 'basic metabolic panel 02012020 417 cst', 'basic metabolic panel 01082021 745', 'basic metabolic panel 12282020 213',
    'basic metabolic panel 01082021 745',
    'basic metabolic panel',
    'basic metabolic panel 01012021 1237', 'basic metabolic panel 216128946 abnormal',
    'comprehensive metabolic panel 01302020 126 pm cst',
    'basic metabolic panel',
    'basic metabolic panel 01022021 429',
    'chemistry basic 36 hours 9',
    'chemistry comprehensive 36 hours 11',
    'chemistry comprehensive 36 hours 12',
    'chemistry comprehensive 36 hours 14',
    'chemistry comprehensive 36 hours 15',
    'chemistry comprehensive 36 hours 16',
    'basic metabolic panel 12282020 213',
    'chemistry comprehensive last 36 hours',
    'chemistry',
    'chemistry comprehensive 36 hours',
    'recent labs',
    'bmp 216586535 abnormal', 'bmp 216506310 Abnormal', 'bmp 216506274 Abnormal',
    'cmp 216227700 abnormal', 'cmp 216506314 abnormal', 'cmp 216465233 Abnormal', 'cmp 216340677 Abnormal', 'cmp 216227700 abnormal', 'cmp 216138914 abnormal',
    'comprehensive metabolic panel',
    'blood chemistry',
    'basic metabolic panel 216128946 abnormal',
    'routine chemistry',
    'labs',
    'lab results',
    'objective',
    'Laboratory Data:'
]
chem_section_list = ['Progress Note', 'Results', 'Chemistry', 'Lab - All Results', 'Neuro ICU Progress Note',
                     'Physician Progress Notes', 'Progress Notes:', 'Hematology', 'General Laboratory', 'Progress Notes - All Other Notes', 'Physical Examination:', 'Laboratory Data:',]
chem_lab_heading = ['Biochemical Data/Procedures/Medical Tests:', 'BASIC METABOLIC PANEL', 'COMPREHENSIVE METABOLIC PANEL',
                    'Chemistry Comprehensive', 'Chemistry Basic', 'bmp 216586535 abnormal', 'cmp 216227700 abnormal', 'Blood Chemistry', 'Components', 'component', 'chemistry',]

chem_lab_table_parser_ls = ['sodium lvl', 'potassium lvl', 'chloride lvl', 'calcium lvl', 'creatinine lvl', 'chlorine lvl',
                            'magnesium lvl', 'phosphate lvl', 'bun lvl', 'glucose lvl', 'bun/creat ratio',
                            'total protein', 'albumin', 'bilirubin total', 'ast', 'alt', 'gfr', 'carbon dioxide', 'anion gap', 'carbon dioxide',
                            'sodium (134 -147 meq/l)', 'potassium (3,4 - 5.0 meq/l)', 'chloride (100 -108 meq/l)', 'carbon dioxide (21 - 33 meq/1)', 'anion gap (0 - 20)', 'bun (7 -18 mg/dl)', 'creatinine (0.6 -1.3 mg/dl)', 'glucose (70-110 mg/dl', 'calcium (8.0 -10.5 mg/dl) ']

chem_table_parser_tag_ls = {'sodium': ['na'], 'potassium': ['k'], 'calcium': ['ca'], 'chlorine': ['ci'],
                            'chloride': ['cl'], 'carbon dioxide': ['co2'], 'glucose': ['blood sugar', 'bs']}

chem_unit_reference = ['x10(3)/uL',
                       'mEq/L',
                       'mg/dL',
                       'gm/L',
                       'thou/mcL',
                       'million/mcL',
                       'mmol/uL',
                       'mmol/L',
                       'fL', 'pg', 'x10(6)/uL', 'g/dL', 'mL/min/1.73m2 ']
