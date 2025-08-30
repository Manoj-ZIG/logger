import pandas as pd

def get_bb(df, date_ls, prev_page_end_index):
    bb_idx, bb_ls = [], []
    for idx, date in enumerate(date_ls):
        _, _, _, date_idx  = date
        start_idx, end_idx = date_idx
        df_idx = start_idx+prev_page_end_index
        meta_data_df = df[df['Start'] <= df_idx].tail(1)
        # bb_idx = df[df['Start'] <= df_idx].iloc[-1]
        l = meta_data_df.iloc[0]['Geometry.BoundingBox.Left'] 
        t = meta_data_df.iloc[0]['Geometry.BoundingBox.Top']
        w = meta_data_df.iloc[0]['Geometry.BoundingBox.Width']
        h = meta_data_df.iloc[0]['Geometry.BoundingBox.Height']
        bb_ls.append((idx, (l, t, w, h)))
    return bb_ls

def vertical_alignment_detection(date_bounding_box):
    df = pd.DataFrame(date_bounding_box, columns=['date_idx', 'bounding_box'])
    pg_df = pd.concat([df.drop('bounding_box', axis=1), pd.DataFrame(df['bounding_box'].tolist(), index=df.index)], axis=1)
    pg_df.columns = ['date_idx', 'Geometry.BoundingBox.Left', 'Geometry.BoundingBox.Top', 'Geometry.BoundingBox.Width', 'Geometry.BoundingBox.Height']
    left_check = [(pg_df['Geometry.BoundingBox.Left'].iloc[i]-pg_df['Geometry.BoundingBox.Left'].iloc[i+1]) < 0.02 for i in range(len(pg_df)-1)]
    top_check = [pg_df['Geometry.BoundingBox.Top'].iloc[i] < pg_df['Geometry.BoundingBox.Top'].iloc[i+1] for i in range(len(pg_df)-1)]
    s1 = pd.Series(top_check) 
    s2 = pd.Series(left_check)
    pg_df['flag'] = s1 & s2
    df_filtered = pg_df[pg_df['flag'] == True]
    df_filtered['group'] = (df_filtered['date_idx'].diff().ne(1)).cumsum()
    longest_seq = df_filtered.groupby("group")['date_idx'].count().max()
    return True if longest_seq > 8 else False