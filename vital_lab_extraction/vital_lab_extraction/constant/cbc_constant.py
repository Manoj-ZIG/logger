wbc_sub_section_list = [
    'cbc', 'orderable name cbc w diff', 'orderable name cbc without diff',
    'cbc with differential 01302020 437 am', 'cbc and automated differential w/reflex 216506313 abnormal',
    'cbc and automated differential w/reflex 216465232 abnormal',
    'cbc and automated differential w/reflex 216340676 abnormal',
    'cbc and automated differential w/reflex 216227699 abnormal',
    'cbc and automated differential w/reflex 216128960 abnormal',
    'hematology basic last 36 hours 25',
    'automated hematology', 'cbcdiff 12272020 1233 am', 'cbc 01052021 455 am',
]
wbc_section_list = ['Progress Note', 'Results', 'Chemistry', 'Lab - All Results', 'Cardiology Consultation Report', 'Labs', 'Consults - All Other Notes',
                    'Physician Progress Notes', 'Progress Notes:', 'Hematology', 'General Laboratory', 'Progress Notes - All Other Notes', 'Physical Examination:', 'Neuro ICU Progress Note']
wbc_exclusion_section_list = ['Past Medical History', 'History and Physical',
                              'Past History', 'Past History Review', 'Past Medical History:', 'Medical History']

wbc_lab_test_master_list = ['wbc', 'rbc', 'hgb', 'hct', 'mcv', 'mch', 'mchc', 'rdw', 'plt', 'inr',
                            'plt ct', 'rdw sd', 'mpv', 'white blood count', 'hemoglobin', 'red blood count', 'hematocrit', '% immature granulocyte',
                            'neutrophil', 'lymphocyte', 'monocyte', 'eosinophil', 'basophil', 'immature granulocyte', 'immature Gran', 'imm gran abs', 'imm gran rel', 'immature granulocyte %', 'granulocyte %', '% immature granulocytes', 'granulocyte', 'imm gran', 'grnn', 'abs imm gran', 'neutrophils', 'lymphocytes', 'monocytes', 'eosinophils', 'basophils',
                            'granulocytes', 'wbc (4.5-11.0 x10 3/uL)', 'rbc (3.54 - 5.02 x10*6/ul',
                            'hgb (11.0 -15.0 g/d', 'hct(33.0 -45.0 %', 'mcv (81.0 -99.0 fL)', 'mch (27.0 - 33.0 pg)',
                            'mchc (33.0 -37.0 g/dl)', 'rdw (11.5-14.5 %)', 'plt count (150 -400 x10 3/ul)',
                            'lymph % (Auto) (14.0 - 32.0 %)']

wbc_lab_header_list = ['component', 'value', 'components', 'Hematology Basic - Last 36 hours (25)', 'Hematology Basic - Last 36 hours (20)',
                       'ref range', 'reference range', 'result', 'date/time', 'hematology-differential', 'differential']

wbc_table_parser_tag_ls = {'wbc': ['wbc', 'white blood count'], 'imm gran': [
    'imm gran', 'immature granulocytes %', 'imm gran abs', 'imm gran rel', 'grnn', 'abs imm gran', 'rel imm gran']}

wbc_unit_reference = ['x10(3)/uL',
                      'mEq/L',
                      'mg/dL',
                      'gm/L',
                      'thou/mcL',
                      'million/mcL',
                      'mmol/uL',
                      'mmol/L',
                      'fL',
                      'pg', 'x10(6)/uL', 'g/dL']
