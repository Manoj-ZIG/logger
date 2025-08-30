import pandas as pd

def check_within_bb(a, b):
    """
    A (Table BB)
    B (component BB)
    if we want to check B lies within A or not
    """
    tA, lA, hA, wA = a
    tB, lB, hB, wB = b
    flag = False if (None in a) or (None in b) else True

    if flag and tA <= tB and lA <= lB and (tA+hA) >= (tB+hB) and (lA+wA) >= (lB+wB):
        return True
    else:
        return False


def align_bb(bb):
    try:
        w, h, t, l = bb
    except Exception as e:
        w, h, t, l = None, None, None, None
    return tuple([t, l, h, w])


def suppress_test_over_tableRegion(df):
    """

    """
    df['IsIntersect'] = 0
    excp_df = df[df['IsFrom'].isin(['excerpt'])]
    excp_df.reset_index(inplace=True, drop=True)
    table_df = df[df['IsFrom'].isin(['table'])]
    table_df.reset_index(inplace=True, drop=True)

    drop_index_ls = []
    for i, row in excp_df.iterrows():
        page = row["Page"]
        testName = row["TestName"]
        testvalue = row["TestResult"]
        sub_table_df = table_df[(table_df['TestName'] == testName) & (table_df['Page'] == page)]
        for j, sub_row in sub_table_df.iterrows():
            if check_within_bb(sub_row['BBInfo'], row['BBInfo']):
                drop_index_ls.append(i)
                excp_df.at[i, 'IsIntersect'] = 1

    excp_df.loc[excp_df[(excp_df['IsDrop'] == 0) & (excp_df['IsIntersect'] == 0)].index, 'IsDrop'] = 0
    excp_df.loc[excp_df[(excp_df['IsDrop'] == 0) & (excp_df['IsIntersect'] == 1)].index, 'IsDrop'] = 1
    # excp_filter_df = excp_df[excp_df['IsIntersect'] == 0]
    excp_intersect_df = excp_df[excp_df['IsIntersect'] == 1]
    resulted_df = pd.concat([table_df, excp_df])
    resulted_df.reset_index(inplace=True, drop=True)
    return resulted_df, excp_intersect_df
