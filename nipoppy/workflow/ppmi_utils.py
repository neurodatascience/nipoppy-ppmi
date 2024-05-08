import warnings
from functools import reduce

import pandas as pd

from nipoppy.workflow.utils import (
    COL_DATATYPE_MANIFEST,
    COL_SESSION_MANIFEST,
    COL_SUBJECT_MANIFEST,
    COL_VISIT_MANIFEST,
)

COL_SUBJECT_TABULAR = 'PATNO'
COL_VISIT_TABULAR = 'EVENT_ID'
COL_GROUP_TABULAR = 'COHORT_DEFINITION'

COL_SUBJECT_IMAGING = 'Subject ID'
COL_SESSION_IMAGING = 'Visit'
COL_GROUP_IMAGING = 'Research Group'
COL_MODALITY_IMAGING = 'Modality'           # column name in PPMI schema
COL_DESCRIPTION_IMAGING = 'Description'
COL_PROTOCOL_IMAGING = 'Imaging Protocol'

MODALITY_DWI = 'DTI'                # PPMI "Modality" column
MODALITY_FUNC = 'fMRI'
MODALITY_ANAT = 'MRI'

VISIT_IMAGING_MAP = {
    'Baseline': 'BL',
    'Month 6': 'R01',
    'Month 12': 'V04',
    'Month 24': 'V06',
    'Month 36': 'V08',
    'Month 48': 'V10',
    'Screening': 'SC',
    'Premature Withdrawal': 'PW',
    'Symptomatic Therapy': 'ST',
    'Unscheduled Visit 01': 'U01',
    'Unscheduled Visit 02': 'U02',
}
GROUP_IMAGING_MAP = {
    'PD': 'Parkinson\'s Disease',
    'Prodromal': 'Prodromal',
    'Control': 'Healthy Control',
    'Phantom': 'Phantom',               # not in participant status file
    'SWEDD': 'SWEDD',
    'GenReg Unaff': 'GenReg Unaff',     # not in participant status file
}

def load_tabular_df(fpath, visits=None, loading_func=None):
    df = pd.read_csv(fpath, dtype=str)
    if loading_func is not None:
        df = loading_func(df)
    df = df.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST,
        COL_VISIT_TABULAR: COL_VISIT_MANIFEST,
    })
    
    if visits is not None:
        df = df[df[COL_VISIT_MANIFEST].isin(visits)]
    return df

def get_tabular_info_and_merge(info_dict, dpath_parent, df_manifest=None, visits=None, loading_func=None):
    merge_how_with_index = 'outer' # 'outer' or 'left' (should be no difference if the index/manifest is correct)
    
    df_static, df_nonstatic = get_tabular_info(info_dict, dpath_parent, visits=visits, loading_func=loading_func)

    if df_nonstatic is None:
        raise RuntimeError('At least one dataframe must contain both subject and visit information')
    elif df_static is None:
        return df_nonstatic
    else:
        # merge again
        check = (df_manifest is not None)
        if df_manifest is None:
            df_manifest = df_nonstatic[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]]
        df_nonstatic = merge_and_check(df_manifest, df_nonstatic, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how=merge_how_with_index, check=check)
        df_static = merge_and_check(df_manifest, df_static, on=[COL_SUBJECT_MANIFEST], how=merge_how_with_index, check=check)
        df_merged = merge_and_check(df_static, df_nonstatic, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how='inner', check=check)
        return df_merged
    
def get_tabular_info(info_dict, dpath_parent, visits=None, loading_func=None):
    dfs_static = [] # no visit info (doesn't change over time)
    dfs_nonstatic = []
    for colname_in_bagel, col_info in info_dict.items():
        is_static = col_info['IS_STATIC'].lower() in ['true', '1', 'yes']
        df = load_tabular_df(dpath_parent / col_info['FILENAME'], visits=(None if is_static else visits), loading_func=loading_func)
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

    return df_static, df_nonstatic
    
def merge_df_list(dfs, on, how='outer') -> pd.DataFrame:
    if len(dfs) == 0:
        df = None
    elif len(dfs) == 1:
        df = dfs[0]
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)
    return df

def merge_and_check(df1: pd.DataFrame, df2: pd.DataFrame, on, how='outer', check=True, check_condition='right_only'):
    col_indicator = '_merge'

    if df2 is None:
        warnings.warn('df2 is None, nothing to merge')
        return df1
    
    df_merged = df1.merge(df2, on=on, how=how, indicator=True)

    if check and (df_merged[col_indicator] == check_condition).any():
        df_check = df_merged.loc[df_merged[col_indicator] == check_condition]
        # df_check.to_csv('df_check.csv', index=False)
        warnings.warn(
            'Tabular dataframes have rows that do not match the manifest'
            '. Something is probably wrong with the manifest'
            f'.\n{df_check}',
            stacklevel=2,
        )

    df_merged = df_merged.drop(columns=[col_indicator])
    return df_merged

def load_and_process_df_imaging(fpath_imaging):

    # load
    df_imaging = pd.read_csv(fpath_imaging, dtype=str)

    # rename columns
    df_imaging = df_imaging.rename(columns={
        COL_SUBJECT_IMAGING: COL_SUBJECT_MANIFEST,
        COL_SESSION_IMAGING: COL_VISIT_MANIFEST,
        COL_DESCRIPTION_IMAGING: COL_DATATYPE_MANIFEST,
    })

    # convert visits from imaging to tabular labels
    try:
        df_imaging[COL_VISIT_MANIFEST] = df_imaging[COL_VISIT_MANIFEST].apply(
            lambda visit: VISIT_IMAGING_MAP[visit]
        )
    except KeyError as ex:
        raise RuntimeError(
            f'Found visit without mapping in VISIT_IMAGING_MAP: {ex.args[0]}')

    # visits and sessions are the same
    df_imaging[COL_SESSION_MANIFEST] = df_imaging[COL_VISIT_MANIFEST]

    # map group to tabular data naming scheme
    try:
        df_imaging[COL_GROUP_TABULAR] = df_imaging[COL_GROUP_IMAGING].apply(
            lambda group: GROUP_IMAGING_MAP[group]
        )
    except KeyError as ex:
        raise RuntimeError(
            f'Found group without mapping in GROUP_IMAGING_MAP: {ex.args[0]}')
    
    return df_imaging
