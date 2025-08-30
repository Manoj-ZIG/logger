vitals_regex = [
    ('Temperature',
     r"\b(?:[tT]emp|[Tt]emperature|T)\b\s?(?:F|is|C|°C|°F|greater than|above|(?:\(!\)))?\s?:{0,2}?\s+(?:\(\d+\.?\d?\s?[FC]?\s?-\s?\d+\.?\d?\s?[FC]?\s?\)?|(?:\[.*?\])?)?\s?\b(\d+\.\d+|\d+)\b\s?(?:°F|°C|Deg|F|Cel|C)?"),
    ('HeartRate',
     r"\b(?:pulse rate|heart rate|hr|pulse)\b\s?\s?(?:is|(?:\(!\)))?:{0,2}?\s+(?:[\(\[]\w.*?[\)\]])?\s?(\b(?:[5-9][0-9]|10[0-9]|11[0-9]|12[0-9])\b)(?![/\d])"),
    ('Spo2',
     r"(?:sp[o0]2):?\s?(?:%)?\s?:{0,2}?\s?(?:is|(?:\(!\)))?\s?:?\s+(?:[\(\[]\w.*?[\)\]])?\s?(:?[4-9][0-9]|100%?)"),
    ('BloodPressure',
     r"(?:bp|b\/p|blood pressure)\s?(?:is|(?::?\s?\(!\)))?\s?:?\s?(?:(?:[\(\[]\w.*?[\)\]])\/?(?:[\(\[]\w.*?[\)\]])?)?\s?(\b(?:\d{2,3}\s?\/\s?\d{2})\b)"),
    ('SystolicBloodPressure',
     r"(?:(?:sys|systolic)\s?(?:blood pressure|bp)|sbp)\s?(?:is|(?::?\s?\(!\)))?\s?:?\s?(?:(?:[\(\[]\w.*?[\)\]])\/?(?:[\(\[]\w.*?[\)\]])?)?\s?(\b(?:\d{2,3})\b)"),
    ('DiastolicPressure',
     r"(?:(?:dia|diastolic)\s?(?:blood pressure|bp)|dbp)\s?(?:is|(?::?\s?\(!\)))?\s?:?\s?(?:(?:[\(\[]\w.*?[\)\]])\/?(?:[\(\[]\w.*?[\)\]])?)?\s?(\b(?:\d{2,3})\b)"),
    ('RespiratoryRate',
     r"(?:(?:resp.?|respiratory|rr|respiration)\s?(?:rate)?|respiration|rr)s?\s?(?:is|(?:\(!\)))?\s?:?\s+(?:[\(\[]\w.*?[\)\]])?\s?(\d{2})"),
    ('O2FlowRate',
     r"\b(?:(?:o2|oxygen) flow rate)\b\s*(?:\(l\s?\/\s?min\)|is|above|(?:\(!\))|[\.]*)?\s*:?\s*(\d{1,2}\.?\d{0,2})\s*(?:l\/min|m)?")
]

vital_master_comp_list = ['blood pressure', 'bp', 'b/p', 'nibp mmhg', 'temperature', 'temp', 'o2 flow rate',
                          'pulse rate', 'hr rate bpm', 'resp rate', 'pulse', 'respiration', 'resp co2 ipm', 'respiratory rate', 'spo2', 'pulse ox', 'heart rate', 'resp']
vital_section_ls = ['consulation report',
                    'Progress Note', 'Physician Progress Note']
vital_table_master_ls = ['Diastolic Blood Pressure',
                         'Systolic Blood Pressure', 'Peripheral Pulse Rate', 'fio2']
vital_unit_ls = ['degC', 'degF', 'bpm', '°F', '°C', '%', 'br/min', 'mm hg']

vital_sub_section_ls = ['vital signs:', 'physical examination']
vital_table_parser_tag_ls = {'blood pressure': ['bp', ],
                             'heart rate': ['hr', 'pulse rate'],
                             'respiratory rate': ['resp',],
                             'temperature': ['temp',],
                             'spo2': ['pulse ox',],
                             'rate of oxygen': ['o2 flow rate','flow rate','oxygen flow rate'],
                             }

vital_reference_range = {'Temperature': (96.8, 101),
                         'HeartRate': (60, 100),
                         'SystolicBloodPressure': (90, 120),
                         'DiastolicBloodPressure': (60, 80),
                         'Spo2': (95, 100),
                         'O2FlowRate': (2, 6),
                         'RespiratoryRate': (12, 20),
                         }