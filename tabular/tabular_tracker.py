#!/usr/bin/env python
import argparse
import json
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

# TODO import from workflow.utils (mr_proc repo)
FPATH_ASSESSMENTS_RELATIVE = Path('tabular/assessments/assessments.csv')
FPATH_BAGEL_RELATIVE = Path('tabular/bagel.csv')
FPATH_DEMOGRAPHICS_RELATIVE = Path('tabular/demographics/demographics.csv')
DNAME_BACKUPS_ASSESSMENTS = '.assessments'
DNAME_BACKUPS_DEMOGRAPHICS = '.demographics'
DNAME_BACKUPS_BAGEL = '.bagels'

def run(fpath_global_config):

    # load the global config
    with Path(fpath_global_config).open('r') as file_global_config:
        global_config = json.load(file_global_config)
    dataset_root = Path(global_config['DATASET_ROOT'])
    fpath_demographics = dataset_root / FPATH_DEMOGRAPHICS_RELATIVE
    fpath_assessments = dataset_root / FPATH_ASSESSMENTS_RELATIVE
    fpath_bagel = dataset_root / FPATH_BAGEL_RELATIVE
    visits = global_config['VISITS']

    # load the manifest
    fpath_manifest = dataset_root / 'tabular' / 'mr_proc_manifest.csv'
    df_manifest = load_manifest(fpath_manifest)

    # initialize tracking info
    df_index = df_manifest[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]].copy()

    # combine demographics info
    df_demographics = process_tabular_and_save(
        global_config['TABULAR']['DEMOGRAPHICS'],
        dataset_root / 'tabular' / 'demographics',
        df_index,
        visits,
        fpath_demographics, 
        DNAME_BACKUPS_DEMOGRAPHICS, 
        'demographics',
    )

    # combine assessments info
    df_assessments = process_tabular_and_save(
        global_config['TABULAR']['ASSESSMENTS'],
        dataset_root / 'tabular' / 'assessments',
        df_index,
        visits,
        fpath_assessments,
        DNAME_BACKUPS_ASSESSMENTS,
        'assessments',
    )

    # combine everything into a single bagel
    df_bagel = df_demographics.merge(df_assessments, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how='outer')
    df_bagel = df_bagel.drop_duplicates().reset_index(drop=True)
    df_bagel.insert(1, COL_BIDS_ID_MANIFEST, df_bagel[COL_SUBJECT_MANIFEST].apply(participant_id_to_bids_id))
    print(f'\nGenerated bagel: {df_bagel.shape}')

    # save bagel
    if fpath_bagel.exists() and pd.read_csv(fpath_bagel, dtype=str).equals(df_bagel):
        print('No changes to bagel file. Will not write new file.')
    else:
        save_backup(df_bagel, fpath_bagel, DNAME_BACKUPS_BAGEL)

def process_tabular_and_save(info_dict, dpath_parent, df_index, visits, fpath, dname_backups, tag):
    df = get_tabular_info(info_dict, dpath_parent, df_index=df_index, visits=visits)
    if Path(fpath).exists() and pd.read_csv(fpath, dtype=str).equals(df):
        print(f'No changes to {tag} file. Will not write new file.')
    else:
        save_backup(df, fpath, dname_backups)
    return df

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

