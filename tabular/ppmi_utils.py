import warnings
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

def get_tabular_info(info_dict, dpath_parent, visits=None):
    dfs_static = [] # no visit info (doesn't change over time)
    dfs = []
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
                warnings.warn(f'Dataframe for column {colname_in_bagel} has a single row per subject but is not marked as static')

            dfs.append(df[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, colname_in_bagel]])

    return dfs, dfs_static
