#!/usr/bin/env python

import argparse
import json
import os
from pathlib import Path
import warnings

import numpy as np
import pandas as pd

DEFAULT_IMAGING_FILENAME = 'idaSearch.csv'
DEFAULT_TABULAR_FILENAME = 'Age_at_visit.csv'

# paths relative to DATASET_ROOT
DPATH_INPUT_RELATIVE = Path('tabular', 'study_data')
DPATH_OUTPUT_RELATIVE = Path('tabular')

COL_SUBJECT_IMAGING = 'Subject ID'
COL_VISIT_IMAGING = 'Visit'
COL_MODALITY_IMAGING = 'Modality'
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
MODALITY_DATATYPE_MAP = {
    'MRI': 'anat', # PPMI modality to BIDS datatype
    'DTI': 'dwi',
    'fMRI': 'func',
}

COL_SUBJECT_TABULAR = 'PATNO'
COL_VISIT_TABULAR = 'EVENT_ID'
VISIT_SESSION_MAP = {
    'BL': '1',
    'V04': '5',
    'V06': '7',
    'V08': '9',
    'V10': '11',
}

# manifest filename and columns
FNAME_MANIFEST = 'mr_proc_manifest.csv'
COL_SUBJECT_MANIFEST = 'participant_id'
COL_DICOM_MANIFEST = 'participant_dicom_dir'
COL_VISIT_MANIFEST = 'visit'
COL_SESSION_MANIFEST = 'session'
COL_DATATYPE_MANIFEST = 'datatype'
COL_BIDS_ID_MANIFEST = 'bids_id'
COLS_MANIFEST = [COL_SUBJECT_MANIFEST, COL_DICOM_MANIFEST, COL_VISIT_MANIFEST, 
                 COL_SESSION_MANIFEST, COL_DATATYPE_MANIFEST, COL_BIDS_ID_MANIFEST]

# global config keys
GLOBAL_CONFIG_DATASET_ROOT = 'DATASET_ROOT'
GLOBAL_CONFIG_SESSIONS = 'SESSIONS'

# flags
FLAG_OVERWRITE = '--overwrite'

def run(global_config_file, imaging_filename, tabular_filename, overwrite=False):

    # parse global config
    with open(global_config_file) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])

    validate_visit_session_map(global_config)

    # generate filepaths
    dpath_input = dpath_dataset / DPATH_INPUT_RELATIVE
    fpath_imaging = dpath_input / imaging_filename
    fpath_tabular = dpath_input / tabular_filename
    fpath_manifest = dpath_dataset / DPATH_OUTPUT_RELATIVE / FNAME_MANIFEST

    # load dfs
    df_imaging = pd.read_csv(fpath_imaging, dtype=str)
    df_tabular = pd.read_csv(fpath_tabular, dtype=str)

    print('Processing imaging data...')

    # rename columns
    df_imaging = df_imaging.rename(columns={
        COL_SUBJECT_IMAGING: COL_SUBJECT_MANIFEST,
        COL_VISIT_IMAGING: COL_VISIT_MANIFEST,
        COL_MODALITY_IMAGING: COL_DATATYPE_MANIFEST,
    })

    # convert visits from imaging to tabular labels
    try:
        df_imaging[COL_VISIT_MANIFEST] = df_imaging[COL_VISIT_MANIFEST].apply(
            lambda visit: VISIT_IMAGING_MAP[visit]
        )
    except KeyError as ex:
        raise RuntimeError(
            f'Found visit without mapping in VISIT_MAP_IMAGING_TO_TABULAR: {ex.args[0]}')

    # map visits to sessions
    df_imaging[COL_SESSION_MANIFEST] = df_imaging[COL_VISIT_MANIFEST].map(VISIT_SESSION_MAP)
    n_rows_orig = df_imaging.shape[0]

    # check if all expected sessions are present
    diff_sessions = set(global_config[GLOBAL_CONFIG_SESSIONS]) - set(df_imaging[COL_SESSION_MANIFEST])
    if len(diff_sessions) != 0:
        warnings.warn(f'Did not encounter all sessions listed in global_config. Missing: {diff_sessions}')

    # only keep sessions that are listed in global_config
    df_imaging = df_imaging.dropna(axis='index', how='any', subset=COL_SESSION_MANIFEST)
    print(
        f'\tDropped {n_rows_orig - df_imaging.shape[0]} imaging entries'
        f' because the session was not in {global_config[GLOBAL_CONFIG_SESSIONS]}'
    )

    # create imaging datatype availability lists
    seen_datatypes = set()
    df_imaging = df_imaging.groupby([COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, COL_SESSION_MANIFEST])[COL_DATATYPE_MANIFEST].aggregate(
        lambda modalities: get_datatype_list(modalities, seen=seen_datatypes)
    )
    df_imaging = df_imaging.reset_index()
    print(f'\tFinal imaging dataframe shape: {df_imaging.shape}')

    # check if all expected datatypes are present
    diff_datatypes = set(MODALITY_DATATYPE_MAP.values()) - seen_datatypes
    if len(diff_datatypes) != 0:
        warnings.warn(f'Did not encounter all datatypes in MODALITY_DATATYPE_MAP. Missing: {diff_datatypes}')
    
    print('Processing tabular data...')

    # rename columns
    df_tabular = df_tabular.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST, 
        COL_VISIT_TABULAR: COL_VISIT_MANIFEST,
    })
    print(f'\tTabular dataframe shape: {df_tabular.shape}')

    # merge on subject and visit
    df_manifest = df_tabular.merge(df_imaging, how='outer', 
                                   on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST])
    
    # replace NA datatype by empty list
    df_manifest[COL_DATATYPE_MANIFEST] = df_manifest[COL_DATATYPE_MANIFEST].apply(
        lambda datatype: datatype if isinstance(datatype, list) else []
    )

    # populate other columns and select/reorder columns used in manifest
    for col in COLS_MANIFEST:
        if not (col in df_manifest.columns):
            df_manifest[col] = np.nan
    df_manifest = df_manifest[COLS_MANIFEST]

    print('Created manifest:')
    print(df_manifest)

    if df_manifest.shape[0] > df_tabular.shape[0]:
        warnings.warn('Some imaging entries have no corresponding tabular information')

    # check if file exists
    if fpath_manifest.exists() and not overwrite:
        raise FileExistsError(f'File exists: {fpath_manifest}. Use {FLAG_OVERWRITE} to overwrite')

    # save file
    df_manifest.to_csv(fpath_manifest, index=False, header=True)
    print(f'File written to: {fpath_manifest}')

    # set file permissions
    os.chmod(fpath_manifest, 0o664)

def validate_visit_session_map(global_config):
    if set(global_config[GLOBAL_CONFIG_SESSIONS]) != set(VISIT_SESSION_MAP.values()):
        raise ValueError(
            f'Invalid VISIT_SESSION_MAP: {VISIT_SESSION_MAP}. Must have exactly one entry'
            f' for each session in global_config: {global_config[GLOBAL_CONFIG_SESSIONS]}')

def get_datatype_list(modalities: pd.Series, seen=None):
    try:
        datatypes = modalities.apply(lambda modality: MODALITY_DATATYPE_MAP[modality])
    except KeyError as ex:
        raise RuntimeError(
            f'Found modality without mapping in VISIT_MAP_IMAGING_TO_TABULAR: {ex.args[0]}')
    datatypes = datatypes.drop_duplicates().sort_values().to_list()
    
    if isinstance(seen, set):
        seen.update(datatypes)

    return datatypes

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Script to generate manifest file for PPMI dataset.
    Requires an imaging data availability info file that can be downloaded from 
    the LONI IDA, as well as the PPMI tabular data availability info file. 
    Both files should be in [DATASET_ROOT]/{DPATH_INPUT_RELATIVE}.
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your mr_proc dataset (required)')
    parser.add_argument(
        '--imaging_filename', type=str, default=DEFAULT_IMAGING_FILENAME,
        help=('name of file containing imaging data availability info, with columns'
              f' "{COL_SUBJECT_IMAGING}", "{COL_VISIT_IMAGING}", and "{COL_MODALITY_IMAGING}"'
              f' (default: {DEFAULT_IMAGING_FILENAME})'))
    parser.add_argument(
        '--tabular_filename', type=str, default=DEFAULT_TABULAR_FILENAME,
        help=('name of file containing tabular data availability info, with columns'
              f' "{COL_SUBJECT_TABULAR}" and "{COL_VISIT_TABULAR}"'
              f' (default: {DEFAULT_TABULAR_FILENAME})'))
    parser.add_argument(
        FLAG_OVERWRITE, action='store_true',
        help=(f'overwrite any existing {FNAME_MANIFEST} file')
    )
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config
    imaging_filename = args.imaging_filename
    tabular_filename = args.tabular_filename
    overwrite = args.overwrite

    run(global_config_file, imaging_filename, tabular_filename, overwrite=overwrite)
