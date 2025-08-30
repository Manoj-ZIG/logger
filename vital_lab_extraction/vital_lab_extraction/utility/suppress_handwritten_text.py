import pandas as pd



def suppress_handwritten_text( sec_subsec_df, textract_df):
    def func(row):
        try :
            relations = eval(row['Relationships'])
            word_id_list = relations[0]['Ids']
            indices =  {index : id for index, id in enumerate(word_id_list) if id not in handwriting_ids}
            relations[0]['Ids'] = list(indices.values())
            new_rel = str(relations)
            words_list = str(row['Text']).split()
            modified_text = [word for i, word in enumerate(words_list) if i in list(indices.keys())]
            modified_text = " ".join(modified_text)   
            return new_rel, modified_text 
        except :
            return row['Relationships'], row['Text']

        
    try :

        handwriting_ids = textract_df[textract_df['TextType'] == 'HANDWRITING']['Id'].to_list()
        sec_subsec_df[['new_relations', 'printed_text']] = sec_subsec_df.apply(func, axis = 1).apply(pd.Series)
        sec_subsec_df.rename(columns={'Text':'old_text', 'printed_text':'Text'}, inplace=True)
        sec_subsec_df.rename(columns={'Relationships':'old_relationships', 'new_relations':'Relationships'}, inplace=True)

    except Exception as e :
        print("Handwritten text is not suppressed", e)
    return sec_subsec_df


