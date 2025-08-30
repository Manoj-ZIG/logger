fp_text = ['discharge information (nf/la only)',
           'cognition assessment',
            'd0100a. provider no.',
            'an order resulted',
            'consult note seen',
            'orders: given',
            'discharge kt',
            'orders:',
            'order?']

section_continue_terms = [
                          'date of service',
                          'document name', 
                          'all recorded',
                          'visit type',
                          'continued',
                          'version', 
                          'group', 
                          'date',
                          'of', 
                          'at']

map_constant = {
    'Plan Care' : 'Plan of Care'
}

section_exclude_text = ['discharge', 'ot plan of care']

section_exclude_patterns = [r'(\d{3,}\s?\border\b)', r'(?:\d{2,}[^0-9A-Za-z]+|see)\s?\bmar\b', r'discharge information (nf/la only)',
                            r'cognition assessment', r'd0100a. provider no.', r'an order resulted', r'consult note seen', r'orders: given',
                            r'discharge kt', r'obtained', r'nsg discharge planning', r'(critical care \(final result\))', r'(on Assessment)',
                            r'flowsheets\s\(taken', r'ordered by', r'(critical care once)', r'\b(procedure \(on)\b',r'(\bphysical therapy \(completed\)\b)',
                            r'(\bconsulting physician notified\b\?)', r'(\boccupational therapy needs\b)', r'\bmar\b\)', r'\blumbar spine surgery\b', 
                            r'(\borders new\b)', r'(\bot plan of care\b)', r'(\bof order\.\b)', r'(\bphysical therapy plan\b)',
                            r'(\bmedication list not\b)', r'(\bwith first mar\b)', r'(\brecent flowsheet documentation\b)',
                            r'(\boccupational therapy plan\b)',r'(\blaboratories\b)', r'(\bmedication administration from\b)', r'(\banesthesia rn\b)',
                            r'(\borders?\b\s?or)', r'(\borders?\b\s?[:?])', r'\:$', r'(\b(?:n\/a|yes|no)\b)',
                            ]