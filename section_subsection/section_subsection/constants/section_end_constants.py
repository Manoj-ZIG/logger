# It should be in the same order
section_end_patterns = ['electronically witnessed',
                        'attestation signed by',
                        'electronically signed',
                        'testing performed by',
                        'electronic signature',
                        'authenticated by',
                        'end of document',
                        'transcribed by',
                        'transcribv by',
                        'transcribe by',
                        'authorized by',
                        'performed by',
                        'addendum by',
                        'consults by',
                        'entered by',
                        'signed by',
                        'edited by',
                        'review by',
                        'verify by',
                        'signed on',
                        'added by',
                        'sign by']

suppress_section_end_pattern = ['performed by', 'attestation signed by', 'added by']

section_end_start_pattern = ['by the following provider\(s\):',' [[Bb]y]?\s*:?\s*', ' EDT', ' EST', ' [[Ss]igned]?\s*:\s*', ' [[Ss]igned]?\s*,\s*', 'SIGNED', ' [[Ee]ditor]?\s*:\s*',
                    '[[Aa]ddendum]?\s*:\s*', 'AM', 'PM', 'PST', 'PDT', 'Signatures?']

creds_pattern = ['M\s*?\.?\s*?D', 'D\s?O', 'DPM', 'PA', 'PA-C', 'CRNA', 'RN',
            'D\s*?\.\s*?O\.', 'PHD', 'MBBS', 'NP', 'APRN', 'DNP-APRN',
            'APRN-CNP', 'OT', 'PT', 'APRN-CNS','PAC', 'COTA', 'RDN', 'RDN, LD', 'ANN', 'LCSW', 'PAS', 'CNA', 'LPTA', 'OTA',
            'APRN-CRNA', 'RPH', 'CPhT', 'APNP', 'PTA',
            ]

# section_end_end_pattern = ['\(', '\d', ' on', ' at', '\[', '\*', 'DATE:']
section_end_end_pattern = ['\bPT\b', '\(', '\d', ' on', ' at', '\[', '\*', 'DATE:', '\bin\b']
