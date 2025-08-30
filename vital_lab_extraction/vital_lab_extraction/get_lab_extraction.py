try:
    from constant.vital_constant import *
    from constant.cbc_constant import *
    from constant.cmp_constant import *
    from constant.blood_culture_constant import *
    from constant.sputum_constant import *
    from constant.urinalysis_constant import *
    from constant.trop_ckmb_constant import *
    from constant.aws_config import aws_access_key_id, aws_secret_access_key
    from vital_extraction.vital_page_detection import VitalPanel as VitalPageExtraction
    from vital_extraction.vital_excerpts import VitalExcerpt
    from lab_extraction.wbc_table_page_detection import WbcPanel as WbcPageExtraction
    from lab_extraction.chemistry_table_page_detection import ChemistryPanel
    from lab_extraction.blood_cult_page_detection import BloodCulture
    from lab_extraction.sputum_page_detection import Sputum
    from lab_extraction.urinalysis_table_page_detection import UrinalysisPanel
    from lab_extraction.trop_ckmb_page_detection import TropCkmb
    from lab_extraction.encephalopathy_page_detection import EncephalopathyLabsPanel
except ModuleNotFoundError as e:
    from .constant.vital_constant import *
    from .constant.cbc_constant import *
    from .constant.cmp_constant import *
    from .constant.blood_culture_constant import *
    from .constant.sputum_constant import *
    from .constant.urinalysis_constant import *
    from .constant.trop_ckmb_constant import *
    from .constant.aws_config import aws_access_key_id, aws_secret_access_key

    from .vital_extraction.vital_page_detection import VitalPanel as VitalPageExtraction
    from .vital_extraction.vital_excerpts import VitalExcerpt
    from .lab_extraction.wbc_table_page_detection import WbcPanel as WbcPageExtraction
    from .lab_extraction.chemistry_table_page_detection import ChemistryPanel
    from .lab_extraction.blood_cult_page_detection import BloodCulture
    from .lab_extraction.sputum_page_detection import Sputum
    from .lab_extraction.urinalysis_table_page_detection import UrinalysisPanel
    from .lab_extraction.trop_ckmb_page_detection import TropCkmb
    from .lab_extraction.encephalopathy_page_detection import EncephalopathyLabsPanel


import json
import logging
import warnings
import boto3
from io import StringIO
warnings.filterwarnings("ignore")

try:   
    s3_c = boto3.client('s3', region_name='us-east-1')
    s3_c.list_buckets()
    print("S3 client initialized successfully using IAM role in get_lab_extraction.py.")
except Exception as e:
    print(f"Failed to initialize S3 client with IAM role: {str(e)} in get_lab_extraction.py.")
    if aws_access_key_id and aws_secret_access_key:
        s3_c = boto3.client('s3', 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
        print("S3 client initialized successfully using manual keys in get_lab_extraction.py.")
    else:
        raise Exception("Unable to initialize S3 client. Check IAM role or provide AWS credentials in get_lab_extraction.py.")

def put_object_to_s3(df,bucket_name, save_path, file_name):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    s3_c.put_object(Bucket=bucket_name, Body=csv_buf.getvalue(
    ), Key=f'{save_path}/{file_name.replace(".csv",".csv")}')
    csv_buf.seek(0)


def get_lab_metadata(bucket_name, sec_subsec_csv, file,path_to_save_logs, detected_labs, constant_dict, sections_constant):
    default_labs = ["Vital", 'CBC', 'CMP']
    keys = default_labs + detected_labs
    return_data = {}
    for lab in keys:
        section_list = sections_constant[sections_constant[lab] == 1]['section_name'].values
        if lab == 'Vital':
            logging.info('FUNCTION START: vital_table_page_detection.py')
            vital_table_page_obj = VitalPageExtraction(
                bucket_name = bucket_name,
                csv_file = sec_subsec_csv,
                lab_test_list=constant_dict.get(
                    lab).get('lab_master_list'),
                # lab_test_list=vital_master_comp_list,
                master_section_list=section_list,
                master_subsection_list=constant_dict.get(
                    lab).get('lab_sub_section_list'))

            vital_meta_data = vital_table_page_obj.get_page_meta_data()
            vital_meta_data_df = vital_table_page_obj.get_page_meta_data_df(
                vital_meta_data)
            vital_table_page_meta_data = vital_table_page_obj.table_checker(
                vital_meta_data_df)
            vital_table_page_list = vital_table_page_obj.get_page_list(
                vital_table_page_meta_data)

            # vital_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/vital_table_page_log_{file}")
            put_object_to_s3(vital_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            return_data[lab] = {'meta_data': vital_table_page_meta_data,
                                'page_list': vital_table_page_list}
            logging.info(
                f'FUNCTION END: vital_table_page_detection.py {vital_table_page_list}')
        elif lab == 'CBC':
            logging.info('FUNCTION START: cbc_table_page_detection.py')
            wbc_obj = WbcPageExtraction(bucket_name=bucket_name, csv_file=sec_subsec_csv,
                                        lab_test_list=constant_dict.get(
                                            lab).get('lab_master_list'),
                                        lab_heading_list=constant_dict.get(
                                            lab).get('lab_heading'),
                                        master_section_list=section_list,
                                        master_subsection_list=constant_dict.get(
                                            lab).get('lab_sub_section_list'),
                                        master_exclusion_section_list=constant_dict.get(
                                            lab).get('lab_exclusion_section_list'))

            wbc_meta_data = wbc_obj.get_page_meta_data()
            wbc_meta_data_df = wbc_obj.get_page_meta_data_df(wbc_meta_data)
            wbc_table_page_meta_data = wbc_obj.table_checker(wbc_meta_data_df)
            wbc_table_page_list = wbc_obj.get_page_list(
                wbc_table_page_meta_data)
            # wbc_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/wbc_table_page_log_{file}")
            return_data[lab] = {'meta_data': wbc_table_page_meta_data,
                                'page_list': wbc_table_page_list}
            put_object_to_s3(wbc_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: cbc_table_page_detection.py {wbc_table_page_list}')
        elif lab == 'CMP':
            #### Chem Panel #####
            logging.info('FUNCTION START: chemistry_table_page_detection.py')
            chem_obj = ChemistryPanel(bucket_name=bucket_name, csv_file=sec_subsec_csv,
                                      lab_test_list=constant_dict.get(
                                          lab).get('lab_master_list'),
                                      lab_heading_list=constant_dict.get(
                                          lab).get('lab_heading'),
                                      master_section_list=section_list,
                                      master_subsection_list=constant_dict.get(
                                          lab).get('lab_sub_section_list'))

            chem_meta_data = chem_obj.get_page_meta_data()
            chem_meta_data_df = chem_obj.get_page_meta_data_df(chem_meta_data)
            chem_table_page_meta_data = chem_obj.table_checker(chem_meta_data_df)
            chem_table_page_list = chem_obj.get_page_list(
                chem_table_page_meta_data)
            # chem_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/chem_table_page_log_{file}")
            
            return_data[lab] = {'meta_data': chem_table_page_meta_data,
                                'page_list': chem_table_page_list}
            put_object_to_s3(chem_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: Chemistry table page detected..{chem_table_page_list}')

        elif lab == 'Blood_culture':
             #### blood culture #####
            logging.info('FUNCTION START: blood_cult_page_detection.py')
            blood_cult_obj = BloodCulture(bucket_name=bucket_name, csv_file=sec_subsec_csv,
                                          lab_test_list=constant_dict.get(
                                              lab).get('lab_master_list'),
                                          lab_heading_list=constant_dict.get(
                                              lab).get('lab_heading'),
                                          master_section_list=section_list,
                                          master_subsection_list=constant_dict.get(
                                              lab).get('lab_sub_section_list'),
                                          master_exclusion_section_list=constant_dict.get(
                                              lab).get('lab_exclusion_section_list')
                                        )

            blood_cult_meta_data = blood_cult_obj.get_page_meta_data()
            blood_cult_meta_data_df = blood_cult_obj.get_page_meta_data_df(
                blood_cult_meta_data)
            blood_cult_table_page_meta_data = blood_cult_obj.table_checker(
                blood_cult_meta_data_df)
            blood_cult_table_page_list = blood_cult_obj.get_page_list(
                blood_cult_table_page_meta_data)
            # blood_cult_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/blood_cult_table_page_log_{file}")
            
            return_data[lab] = {'meta_data': blood_cult_table_page_meta_data,
                                'page_list': blood_cult_table_page_list}
            put_object_to_s3(blood_cult_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: Blood culture table page detected..{blood_cult_table_page_list}')

        elif lab == 'Sputum':
            #### sputum culture #####
            logging.info('FUNCTION START: sputum_page_detection.py')
            sputum_obj = Sputum(bucket_name=bucket_name, csv_file=sec_subsec_csv,
                                lab_test_list=constant_dict.get(
                                    lab).get('lab_master_list'),
                                lab_heading_list=constant_dict.get(
                                    lab).get('lab_heading'),
                                master_section_list=section_list,
                                master_subsection_list=constant_dict.get(
                                    lab).get('lab_sub_section_list'),
                                master_exclusion_section_list=constant_dict.get(
                                    lab).get('lab_exclusion_section_list')
                                )
            sputum_meta_data = sputum_obj.get_page_meta_data()
            sputum_meta_data_df = sputum_obj.get_page_meta_data_df(
                sputum_meta_data)
            sputum_table_page_meta_data = sputum_obj.table_checker(
                sputum_meta_data_df)
            sputum_table_page_list = sputum_obj.get_page_list(
                sputum_table_page_meta_data)
            # sputum_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/sputum_table_page_log_{file}")
            return_data[lab] = {'meta_data': sputum_table_page_meta_data,
                                'page_list': sputum_table_page_list}
            put_object_to_s3(sputum_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: sputum table page detected..{sputum_table_page_list}')

        elif lab == 'Urinalysis':
            #### urinalysis page detection #####
            logging.info('FUNCTION START: urinalysis_page_detection.py')
            urinalysis_obj = UrinalysisPanel(bucket_name=bucket_name, csv_file=sec_subsec_csv, 
                                             lab_heading_list=constant_dict.get(
                                                 lab).get('lab_heading'),
                                             lab_test_list=constant_dict.get(
                                                 lab).get('lab_master_list'),
                                             master_section_list=section_list,
                                             master_subsection_list=constant_dict.get(
                                                 lab).get('lab_sub_section_list')
                                            )
            urinalysis_meta_data = urinalysis_obj.get_page_meta_data()
            urinalysis_meta_data_df = urinalysis_obj.get_page_meta_data_df(
                urinalysis_meta_data)
            urinalysis_table_page_meta_data = urinalysis_obj.table_checker(
                urinalysis_meta_data_df)
            urinalysis_table_page_list = urinalysis_obj.get_page_list(
                urinalysis_table_page_meta_data)
            # urinalysis_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/urinalysis_table_page_log_{file}")
            return_data[lab] = {'meta_data': urinalysis_table_page_meta_data,
                                'page_list': urinalysis_table_page_list}
            put_object_to_s3(urinalysis_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: urinalysis table page detected..{urinalysis_table_page_list}')
    
        elif lab == 'Trop_ckmb':
            logging.info(
                'FUNCTION START: tropCkmb_table_page_detection.py')
            TropCkmb_table_page_obj = TropCkmb(
                bucket_name=bucket_name,
                csv_file=sec_subsec_csv,
                lab_test_list=constant_dict.get(
                    lab).get('lab_master_list'),
                master_section_list=section_list,
                master_subsection_list=constant_dict.get(
                    lab).get('lab_sub_section_list'))
            
            tc_meta_data = TropCkmb_table_page_obj.get_page_meta_data()
            tc_meta_data_df = TropCkmb_table_page_obj.get_page_meta_data_df(
                tc_meta_data)
            tc_table_page_meta_data = TropCkmb_table_page_obj.table_checker(
                tc_meta_data_df)
            tc_table_page_list = TropCkmb_table_page_obj.get_page_list(
                tc_table_page_meta_data)
            # tc_table_page_meta_data.to_csv(
            #     rf"{path_to_save_logs}/trop_ckmb_table_page_log_{file}")
            return_data[lab] = {'meta_data': tc_table_page_meta_data,
                                'page_list': tc_table_page_list}
            put_object_to_s3(tc_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: tropCkmb_table_page_detection.py {tc_table_page_list}')
        elif lab == 'Encephalopathy_labs':
            #### Encephalopathy Panel #####
            logging.info('FUNCTION START: Encephalopathy_table_page_detection.py')
            enceph_obj = EncephalopathyLabsPanel(bucket_name=bucket_name, csv_file=sec_subsec_csv,
                                      lab_test_list=constant_dict.get(
                                          lab).get('lab_master_list'),
                                      lab_heading_list=constant_dict.get(
                                          lab).get('lab_heading'),
                                      master_section_list=section_list,
                                    master_subsection_list=constant_dict.get(lab).get('lab_sub_section_list') )

            enceph_meta_data = enceph_obj.get_page_meta_data()
            enceph_meta_data_df = enceph_obj.get_page_meta_data_df(enceph_meta_data)
            enceph_table_page_meta_data = enceph_obj.table_checker(enceph_meta_data_df)
            enceph_table_page_list = enceph_obj.get_page_list(
                enceph_table_page_meta_data)
            
            return_data[lab] = {'meta_data': enceph_table_page_meta_data,
                                'page_list': enceph_table_page_list}
            put_object_to_s3(enceph_table_page_meta_data,
                             bucket_name, path_to_save_logs, f"{lab}_table_page_log_{file}")
            logging.info(
                f'FUNCTION END: Encephalopathy_table_page_detection..{enceph_table_page_list}')
    #adding the sequence page no of table detected pages to lab page list
    # for lab_test, mdata in return_data.items():
    #     page_list = mdata.get('page_list')
    #     sequence_pages = []
    #     for page_no in page_list:
    #         sequence_pages.append(page_no+1)
    #     page_list.extend(sequence_pages)
    #     return_data[lab_test]['page_list'] = set(page_list)
    # ##### saving the logs ########
    log_dict = {k: v.get('page_list') for (k, v) in return_data.items()}
    s3_c.put_object(Bucket=bucket_name, Body=str(log_dict),
                    Key=rf"{path_to_save_logs}/table_page_list_log.txt")
    # with open(rf"{path_to_save_logs}/table_page_list_log.txt", "w") as f:
    #     f.write(json.dumps(log_dict))

    return return_data