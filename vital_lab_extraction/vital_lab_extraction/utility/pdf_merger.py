import fitz
import boto3
from io import BytesIO


class PdfMerger:
    """ 
    call merged_pdf() on PdfMerger object
    """

    def __init__(self):
        pass

    @staticmethod
    def merged_pdf(s3_r, bucket_name, path_to_save, page_ls, doc_path):
        """  
        args: str:  file_name (xxxxxxxxx.pdf)
              str:  path_to_save
              list:  page_ls
              str:  pdf_doc_path
        return:
                None
        """
        page_ls = sorted(list(set(page_ls)))
        page_ls_ = [i-1 for i in page_ls]
        try:
            # merged_pdf = fitz.open()
            pdf_obj = s3_r.Object(
                bucket_name, doc_path)
            pdf_obj_body = pdf_obj.get()["Body"].read()
            pdf_doc = fitz.open("pdf", stream=BytesIO(pdf_obj_body))
            # for page_ in page_ls:
            #     # pdf_doc = fitz.open(doc_path)
            #     page = pdf_doc.load_page(page_ - 1)
            #     merged_pdf.insert_pdf(
            #         pdf_doc, from_page=page.number, to_page=page.number)
            total_pages=len(pdf_doc)
            if total_pages == page_ls_[-1]:
                del page_ls_[-1]
            pdf_doc.select(page_ls_)  # select the 1st & 2nd page of the document
            # new_bytes = merged_pdf.write()
            new_bytes = pdf_doc.write()
            s3_r.Bucket(bucket_name).put_object(
                Key=f'{path_to_save}/{doc_path.split("/")[-1].replace(".pdf","")}_textract_table_merged.pdf', Body=new_bytes)
            pdf_doc.close()
            return 'save merged pdf'
        except Exception as e:
            print(e)
            return 'failed'
      
