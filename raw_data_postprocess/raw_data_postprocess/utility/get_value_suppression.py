import logging
import warnings
import pandas as pd
warnings.filterwarnings("ignore")

ref_rg = {'Temperature': [80, 99],
        'Pulse': [50, 120],
        'Blood Pressure - Systolic': [60, 600],
        'Blood Pressure - Diastolic': [40, 150],
        'SpO2': [80, 100],
        'Rate of Oxygen': [0, 80],
        'Respiration': [6, 30],
        'Fever': [80, 99],
        'WBC': [0.1, 120000],
        'Bands (i.e. left-shift)': [0, 20],
        'Sodium': [90, 200],
        'Potassium': [0.5, 10],
        'Creatine Kinase': [10, 1000],
        'Serum Creatinine': [0.2, 9],
        'Bilirubin': [0, 20],
        'Blood Urea Nitrogen (BUN)': [0, 150],
        'Urine Specific Gravity': [1, 10],
        'Urine Osmolality': [0, 2000],
        'Urine Sodium': [10, 150],
        'Urine Creatinine': [0, 90],
        'Urine Protein': [0, 30],
        'Cardiac Troponin': [0, 400],
        'Procalcitonin': [0.1, 20],
        'C-Reactive Protein (CRP)': [0.1, 400],
        "Urine Volume (e.g. I&O's)": [0.5, 100],
        'Mean Arterial Pressure': [50, 305],
        'PT': [0, 40],
        'INR': [0, 10],
        'APTT': [20, 50],
        'Platelets': [10000, 1000000],
        'FiO2': [20, 100],
        'Glasgow Coma Scale (GCS)': [0, 15],
        'Thiamine Test': [0, 300],
        'Vitamin B12': [0, 1500],
        'Ammonia Level Test': [0, 120],
        'GFR Test': [0, 160],
        'Serum Cortisol': [0, 75],
        'RBC': [1, 20],
        'Hemoglobin Test': [1, 40],
        'Hematocrit Test': [5, 90],
        'Direct Bilirubin': [0, 10],
        'Alanine Transaminase (ALT)': [0, 1500],
        'Aspartate Transaminase (AST)': [0, 10000],
        'Alkaline Phosphatase': [0, 1500],
        'Albumin': [0.5, 15],
        'Total Protein': [1, 30],
        'Gamma-glutamyltransferase (GGT)': [1, 2500],
        'Chloride': [50, 200],
        'Alcohol Level': [0, 80],
        'Acetaminophen Test': [0, 1000],
        'Thyroid Stimulating Hormone (TSH)': [0, 30],
        'Free Thyroxine (FT4)': [0, 10],
        'Thyroxine (T4)': [0, 4],
        'Triiodothyronine (FT3)': [0, 30],
        'Relative Lymphocytes': [0, 80],
        'Absolute Lymphocytes': [0, 20],
        'Lactate Dehydrogenase (LDH)': [50, 2000],
        'Uric Acid, Serum': [0, 30],
        'Uric Acid, Urine': [2.4, 7.2],
        'PaO2, Arterial': [30, 100],
        'PaCO2': [5, 90],
        'HCO3': [1, 50],
        'Glucose': [10, 1000],
        'Calcium': [1, 50],
        'Magnesium': [0.1, 5],
        'Phosphate': [5, 200],
        'Carbon Dioxide': [5, 80],
        'PTT': [0.5, 150],
        'Baseline Serum Creatinine':[0.5, 2]
        }


def convert_str_float(val_str):
    """
    utility function to convert possible float/int from str
    '5.0' --> 5.0
    '5.kf' --> '5.kf'
    """
    if val_str:
        try:
            return float(val_str)
        except ValueError as e:
            return val_str
    else:
        return val_str
def suppress_value(postprocess_df):
    """
    function use to suppress the anamoly values of test which get assign while extraction.
    e.g. Temperature:150 F --> will get suppress as it's limit range is [80-99]
    
    args:
        pandas df: postprocess_df
    return:
        pandas_df: suppress dataframe (of postprocess)
    """
    logging.info(f'FUNCTION START')
    if postprocess_df.shape[0] > 0:
        postprocess_df_copy = postprocess_df.copy()
        postprocess_df_copy['label'] = postprocess_df_copy.apply(lambda x: (0.5*ref_rg.get(str(x.TestName))[0]) < convert_str_float(x.TestResult) < (1.5*ref_rg.get(str(x.TestName))[1]) if x.TestName in ref_rg.keys() and isinstance(convert_str_float(x.TestResult), (int, float)) else False,
                                    axis=1)
        postprocess_value_suppress_df = postprocess_df_copy[postprocess_df_copy['label'] == True].copy(
        )
        postprocess_value_suppress_df.drop(columns=['label'], inplace=True)
        print(f"Before value suppresion (postprocess_df): {postprocess_df.shape} \
                After value suppresion (postprocess_df): {postprocess_value_suppress_df.shape}")
        
        postprocess_df_copy.loc[postprocess_df_copy[(postprocess_df_copy['label'] == True) & (postprocess_df_copy['IsDrop'] == 0)].index, 'IsDrop'] = 0
        postprocess_df_copy.loc[postprocess_df_copy[(postprocess_df_copy['label'] == False) & (postprocess_df_copy['IsDrop'] == 0)].index, 'IsDrop'] = 1
        logging.info(f'Before value suppresion (postprocess_df) : {postprocess_df.shape}')
        logging.info(f'After value suppresion (postprocess_df) : {postprocess_value_suppress_df.shape}')
        # return postprocess_value_suppress_df.reset_index(drop=True)
        logging.info(f'FUNCTION END')
        return postprocess_df_copy
    else:
        logging.info(f'FUNCTION END')
        return pd.DataFrame({})