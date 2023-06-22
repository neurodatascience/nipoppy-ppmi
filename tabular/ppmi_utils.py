import warnings
from functools import reduce

import pandas as pd

from workflow.utils import (
    COL_SUBJECT_MANIFEST,
    COL_VISIT_MANIFEST,
)

COL_SUBJECT_TABULAR = 'PATNO'
COL_VISIT_TABULAR = 'EVENT_ID'
COL_GROUP_TABULAR = 'COHORT_DEFINITION'

def load_tabular_df(fpath, visits=None):
    df = pd.read_csv(fpath, dtype=str)
    df = df.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST,
        COL_VISIT_TABULAR: COL_VISIT_MANIFEST,
    })
    
    if visits is not None:
        df = df[df[COL_VISIT_MANIFEST].isin(visits)]
    return df

def get_tabular_info(info_dict, dpath_parent, df_index=None, visits=None):
    merge_how_with_index = 'outer' # 'outer' or 'left' (should be no difference if the index/manifest is correct)
    dfs_static = [] # no visit info (doesn't change over time)
    dfs_nonstatic = []
    for colname_in_bagel, col_info in info_dict.items():
        is_static = col_info['IS_STATIC'].lower() in ['true', '1', 'yes']
        df = load_tabular_df(dpath_parent / col_info['FILENAME'], visits=(None if is_static else visits))
        df = df.rename(columns={col_info['COLUMN']: colname_in_bagel})
        # df = df.dropna(axis='index', how='any', subset=colname_in_bagel) # drop rows with missing values

        if is_static:
            dfs_static.append(df[[COL_SUBJECT_MANIFEST, colname_in_bagel]])
        else:
            # sanity check
            if len(df.groupby(COL_SUBJECT_MANIFEST)[COL_SUBJECT_MANIFEST].count().drop_duplicates()) == 1:
                warnings.warn(
                    f'Dataframe for column {colname_in_bagel} has a single row'
                    ' per subject but is not marked as static',
                    stacklevel=2,
                )
            dfs_nonstatic.append(df[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, colname_in_bagel]])

    # merge
    df_static = merge_df_list(dfs_static, on=[COL_SUBJECT_MANIFEST], how='outer')
    df_nonstatic = merge_df_list(dfs_nonstatic, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how='outer')

    if df_nonstatic is None:
        raise RuntimeError('At least one dataframe must contain both subject and visit information')
    elif df_static is None:
        return df_nonstatic
    else:
        # merge again
        if df_index is None:
            df_index = df_nonstatic[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]]
        df_nonstatic = merge_and_check(df_index, df_nonstatic, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how=merge_how_with_index)
        df_static = merge_and_check(df_index, df_static, on=[COL_SUBJECT_MANIFEST], how=merge_how_with_index)
        df_merged = merge_and_check(df_static, df_nonstatic, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how='inner')
        return df_merged
    
def merge_df_list(dfs, on, how='outer') -> pd.DataFrame:
    if len(dfs) == 0:
        df = None
    elif len(dfs) == 1:
        df = dfs[0]
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)
    return df

def merge_and_check(df1: pd.DataFrame, df2: pd.DataFrame, on, how='outer', check_condition='right_only'):
    col_indicator = '_merge'

    if df2 is None:
        warnings.warn('df2 is None, nothing to merge')
        return df1
    
    df_merged = df1.merge(df2, on=on, how=how, indicator=True)

    if (df_merged[col_indicator] == check_condition).any():
        warnings.warn(
            'Tabular dataframes have rows that do not match the manifest'
            '. Something is probably wrong with the manifest'
            f'.\n{df_merged.loc[df_merged[col_indicator] == check_condition]}',
            stacklevel=2,
        )

    df_merged = df_merged.drop(columns=[col_indicator])
    return df_merged