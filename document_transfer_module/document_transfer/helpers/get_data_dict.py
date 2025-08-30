def get_data_dict(data):
    document_storage_path, document_name_cleaned, document_size, document_type_code,\
         is_arl_present_in_document_name, is_arl_matched_with_document, is_document_sent_for_manual_review, is_document_duplicated = data
    data_dict = {'document_storage_path': document_storage_path,
                'document_name_cleaned': document_name_cleaned,
                'document_size': document_size,
                'document_type_code':document_type_code,
                'is_arl_present_in_document_name':is_arl_present_in_document_name,
                'is_arl_matched_with_document':is_arl_matched_with_document,                                                       
                'is_document_sent_for_manual_review': is_document_sent_for_manual_review,
                'is_document_duplicated': is_document_duplicated
                }
    return data_dict