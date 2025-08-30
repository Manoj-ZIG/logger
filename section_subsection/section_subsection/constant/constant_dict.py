creds_dict = {
    'MD':['MD', 'M D', 'M.D', 'M. D', 'M . D', 'M .D', 'M.D.', 'M. D.', 'M D.', 'M . D.', 'M .D.'],
    'DO':['DO', 'D O', 'D O.', 'D.O.', 'D. O.', 'D . O.'],
    'DPM':['DPM'],
    'PA':['PA'],
    'PA-C':['PA-C'],
    'PHD':['PHD'],
    'DNP-APRN':['DNP-APRN'],
    'APRN':['APRN'],
    'CRNA':['CRNA']
}
creds_rank = {
 'MD': 1,
 'DO': 1,
 'DPM': 1,
 'DC': 1,
 'PA': 2,
 'PA-C': 2,
 'DNP-APRN': 3,
 'APRN': 3,
 'CRNA': 3}

mapped_template = {'AKI': 'Acute Kidney Injury', 'ATN': 'ATN',
                       'PNA': 'Pneumonia', 'MISC': 'Miscellaneous', 'AMI': 'AMI'}