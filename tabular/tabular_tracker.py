#!/usr/bin/env python
import argparse
import json
import warnings
from functools import reduce
from pathlib import Path

import pandas as pd

from workflow.utils import (
    COL_BIDS_ID_MANIFEST,
    COL_SUBJECT_MANIFEST,
    COL_VISIT_MANIFEST,
    load_manifest,
    save_backup,
    participant_id_to_bids_id,
)
from tabular.ppmi_utils import get_tabular_info

FPATH_BAGEL_RELATIVE = Path('tabular/bagel.csv')
DNAME_BACKUPS_BAGEL = '.bagels' # TODO import from workflow.utils (mr_proc repo)

def run(fpath_global_config):

    # load the global config
    with Path(fpath_global_config).open('r') as file_global_config:
        global_config = json.load(file_global_config)
    dataset_root = Path(global_config['DATASET_ROOT'])
    fpath_bagel = dataset_root / FPATH_BAGEL_RELATIVE
    visits = global_config['VISITS']

    # load the manifest
    fpath_manifest = dataset_root / 'tabular' / 'mr_proc_manifest.csv'
    df_manifest = load_manifest(fpath_manifest)

    # initialize tracking info
    df_bagel = df_manifest[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]].copy()

    dfs_demographics, dfs_demographics_static = get_tabular_info(
        global_config['TABULAR']['DEMOGRAPHICS'],
        dataset_root / 'tabular' / 'demographics',
        visits=visits,
    )

    dfs_assessments, dfs_assessments_static = get_tabular_info(
        global_config['TABULAR']['ASSESSMENTS'],
        dataset_root / 'tabular' / 'assessments',
        visits=visits,
    )

    # merge subject-visit dfs first
    # then merge with subject-only dfs
    df_static = merge_dfs(dfs_demographics_static + dfs_assessments_static, merge_on=[COL_SUBJECT_MANIFEST])
    df_non_static = merge_dfs(dfs_demographics + dfs_assessments, merge_on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST])

    # print('non-static:')
    # print(df_non_static)
    # print('\nstatic:')
    # print(df_static)
    # df_static.to_csv('df_static.csv', index=False)
    # df_non_static.to_csv('df_non_static.csv', index=False)

    # df_all = df_non_static.merge(df_static, on=COL_SUBJECT_MANIFEST, how='outer')

    # print('\nall:')
    # print(df_all)
    # df_all.to_csv('df_all.csv', index=False)

    # df_bagel = add_cols_to_bagel_and_check(df_bagel, df_all, merge_on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], merge_how='left')
    # df_bagel = add_cols_to_bagel_and_check(df_bagel, df_non_static, merge_on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], merge_how='outer')

    # for participant_id in df_static[COL_SUBJECT_MANIFEST]:

    df_bagel = add_cols_to_bagel_and_check(df_bagel, df_non_static, merge_on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], merge_how='outer')
    df_bagel = add_cols_to_bagel_and_check(df_bagel, df_static, merge_on=[COL_SUBJECT_MANIFEST], merge_how='left')
    
    df_bagel = df_bagel.drop_duplicates()
    df_bagel.insert(1, COL_BIDS_ID_MANIFEST, df_bagel[COL_SUBJECT_MANIFEST].apply(participant_id_to_bids_id))
    # df_bagel[COL_BIDS_ID_MANIFEST] = df_bagel[COL_SUBJECT_MANIFEST].apply(participant_id_to_bids_id)
    print(f'Generated bagel: {df_bagel.shape}')

    # TODO do not write new bagel if no changes were made

    # save bagel
    save_backup(df_bagel, fpath_bagel, DNAME_BACKUPS_BAGEL)

def merge_dfs(dfs, merge_on, merge_how='outer'):
    if len(dfs) == 0:
        df = None
    elif len(dfs) == 1:
        df = dfs[0]
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=merge_on, how=merge_how), dfs)
    return df

def add_cols_to_bagel_and_check(df_bagel, df_to_add, merge_on, merge_how='outer'):
    col_indicator = '_merge'

    if df_to_add is None:
        return df_bagel
    
    df_merged = df_bagel.merge(df_to_add, on=merge_on, how=merge_how, indicator=True)

    if (df_merged[col_indicator] == 'right_only').any():
        warnings.warn(
            'Tabular dataframes have rows that do not match the manifest'
            '. Something is probably wrong with the manifest'
            f'.\n{df_merged.loc[df_merged[col_indicator] == "right_only"]}')

    df_merged = df_merged.drop(columns=[col_indicator])
    return df_merged

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    TODO
    """

    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your mr_proc dataset (required)')
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config

    run(global_config_file)

