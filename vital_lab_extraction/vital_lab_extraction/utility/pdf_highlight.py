import fitz
import logging




class HighlightPdf:
    def __init__(self, pdf_doc):
        self.pdf_doc = pdf_doc

    def highlight_text(self, page_obj, page_no, bb_info, color):
        """
        the highlight_text function used to add highlighted marker on text using bb_info.
        args:
            fitz.obj = page object
            int: page_no
            tuple: bb_info
            tuple: color (RGB)
        return:
            None
        """
        w, h, t, l = bb_info
        left = l * page_obj.rect.width
        top = t * page_obj.rect.height
        width = w * page_obj.rect.width
        height = h * page_obj.rect.height

        excp_inst = [
            fitz.Rect(left, top, left + width, top + height)]

        # rgb to percentage value
        r, g, b = color
        color_rgb = (r/255, g/255, b/255)
        try:
            for inst in excp_inst:
                highlight = page_obj.add_highlight_annot(inst)
                highlight.set_colors(
                    {"stroke": color_rgb, })

                highlight.update()
                page = self.pdf_doc.load_page(page_no)
        except Exception as e:
            pass

    def highlight_pdf(self, result_df_, doc, file, path_to_save, s3_r, bucket_name):
        """
        the main function for highlighting given pdf.
        args:
            df = result_df_ (contain metadata info, like BB,page)
            fitz.obj: doc
            str: file
            str: path_to_save  
        return:
            None
        """
        highlighted_quad = []

        pgs = result_df_['Page'].to_list()
        pgs_ = []
        for pg in pgs:
            if pg:
                if str(pg) != 'nan':
                    pgs_.append(int(pg))
        pgs_ = sorted(list(set(pgs_)))
        for pg in pgs_:
            page_obj = doc[pg-1]
            sub_df = result_df_[result_df_['Page'] == pg]
            for idx, row in sub_df.iterrows():
                color_ = row['Color']
                # color_ = row['Color']
                if int(row['isNegated']) == 0:
                    color_ = [255, 127, 127] #red
                bbox_list = sum(row['GenericTermBB'] +
                                row['SpecificTermBB'], [])
                for bbox in bbox_list:
                    if bbox not in highlighted_quad:
                        self.highlight_text(page_obj, pg-1, bbox, color_)
                    highlighted_quad.append(bbox)
        new_bytes = doc.write()
        s3_r.Bucket(bucket_name).put_object(
            Key=f'{path_to_save}/{file.split("/")[-1].replace(".pdf","")}_highlighted.pdf', Body=new_bytes)
        doc.close()
        
        # copy the pdf to UI/path
        copy_source = {'Bucket': bucket_name,
                       'Key': f'{path_to_save}/{file.split("/")[-1].replace(".pdf","")}_highlighted.pdf'}
        return copy_source
