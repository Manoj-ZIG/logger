import fitz
import json
import trp as t1
import urllib.parse
import trp.trp2 as t2
from io import BytesIO, StringIO
from trp.t_pipeline import add_page_orientation

class PDFRotation:
    def __init__(self, s3c, s3r):
        self.s3c = s3c
        self.s3r = s3r

    def round_to_nearest_90(self, n):
        return round(n / 90) * 90
    
    def delete_line_numbers_and_merge_blocks(self, json_dict):
        """
        ### The function deletes the key LINE_NUMBER and merges the other block to 0th block
        args:
            json_dict: json
        return:
            json_dict: json
        """
        for i in json_dict:
            for block in range(len(i['Blocks'])):
                if "LINE_NUMBER" in i['Blocks'][block].keys():
                    del i['Blocks'][block]['LINE_NUMBER']
        
        for i in range(1, len(json_dict)):
            json_dict[0]['Blocks'].extend(json_dict[i]['Blocks'])
        
        return json_dict

    def get_page_angle(self, json_dict, save_path_bucket, json_path_to_save_result, json_file_name):
        """
        ### The function is used to load the json and get the page orientation
        args:
            json_dict: json
            save_path_bucket: bucket name
            json_path_to_save_result: key
            json_file_name: json_file_name
        return:
            rotation_res: dict
        """
        # assign the Textract JSON dict to j
        # j = <call_textract(input_document="path_to_some_document (PDF, JPEG, PNG)") or your JSON dict>
        t_document: t2.TDocument = t2.TDocumentSchema().load(json_dict[0])
        print("--- Loaded json ---")
        t_document = add_page_orientation(t_document)
        print("--- Add page Orientation --- ")

        doc = t1.Document(t2.TDocumentSchema().dump(t_document))
        # page orientation can be read now for each page
        count = 1
        rotation_res = {}
        for page in doc.pages:
            # print(page.custom['PageOrientationBasedOnWords'])
            rotation_res[count] = page.custom['PageOrientationBasedOnWords']
            count += 1
        json_data = {"MetaData":rotation_res}

        put_resp_flag = self.s3c.put_object(Bucket=save_path_bucket, Body=json.dumps(json_data),
                                            Key=rf'{json_path_to_save_result}/{json_file_name}')
        return rotation_res

    def rotate_pdf(self, file_name, angle_dict, bucket_name, pdf_doc_path, pdf_path_to_save_result, save_path_ui_bucket, path_to_save_result_ui):
        """
        ### The function is used to load the json and get the page orientation
        args:
            angle_dict: json
            bucket_name: bucket name
            pdf_doc_path: highlighted pdf doc path
            pdf_path_to_save_result: rotated highlighted pdf save path
            save_path_ui_bucket: bucket name (ui/application)
            path_to_save_result_ui: pdf path to save result
        return:
            None
        """
        # Open the PDF
        pdf_obj = self.s3r.Object(
                    bucket_name, pdf_doc_path)
        pdf_obj_body = pdf_obj.get()["Body"].read()
        pdf_document = fitz.open("pdf", stream=BytesIO(pdf_obj_body))
        print("Number of pages :", pdf_document.page_count)
        # Rotate each page
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            # degrees = 360+angle_dict[page_num+1] if angle_dict[page_num+1] < 0 else angle_dict[page_num+1]
            current_rot = page.rotation
            try:
                more_rot = angle_dict[page_num+1]
                more_rot = self.round_to_nearest_90(more_rot)
                # print(page_num+1, angle_dict[page_num+1], page.rotation, more_rot)
                more_rot = more_rot*(-1) if more_rot < 0 else more_rot
                # more_rot = 360-current_rot
                if more_rot == 90:
                    more_rot += 180
                page.set_rotation(more_rot)
            except Exception as e:
                print(e)
        new_bytes = pdf_document.write()
    
        # Save the rotated PDF
        self.s3r.Bucket(bucket_name).put_object(
                    Key=f'{pdf_path_to_save_result}/{file_name}', Body=new_bytes)
        copy_source = {'Bucket': bucket_name,
                        'Key': f'{pdf_path_to_save_result}/{file_name}'}
        print(f"output pdf saved at {copy_source}")
        content_type='application/pdf'
        self.s3c.copy_object(CopySource=copy_source, Bucket=save_path_ui_bucket,
                            Key=f'{path_to_save_result_ui}', ContentType=content_type,
                            MetadataDirective='REPLACE')
        print(f'copied {copy_source} to the key:  {path_to_save_result_ui}')