#!/usr/bin/env python

import argparse
import datetime
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from tabular.filter_image_descriptions import COL_DESCRIPTION as COL_DESCRIPTION_IMAGING
from tabular.filter_image_descriptions import FNAME_DESCRIPTIONS, DATATYPE_ANAT, DATATYPE_DWI, DATATYPE_FUNC

# subject groups to keep
GROUPS_KEEP = ['Parkinson\'s Disease', 'Prodromal', 'Healthy Control', 'SWEDD', 'GenReg Unaff']

# paths relative to DATASET_ROOT
DPATH_INPUT_RELATIVE = Path('tabular', 'study_data')
DPATH_OUTPUT_RELATIVE = Path('tabular')

DEFAULT_IMAGING_FILENAME = 'idaSearch.csv'
DEFAULT_TABULAR_FILENAMES = [
    'Age_at_visit.csv', 
    'Montreal_Cognitive_Assessment__MoCA_.csv',
    'MDS-UPDRS_Part_I.csv',
    'MDS-UPDRS_Part_I_Patient_Questionnaire.csv',
    'MDS_UPDRS_Part_II__Patient_Questionnaire.csv',
    'MDS-UPDRS_Part_III.csv',
    'MDS-UPDRS_Part_IV__Motor_Complications.csv',
]
DEFAULT_GROUP_FILENAME = 'Participant_Status.csv'
DEFAULT_DPATH_BACKUPS_RELATIVE = DPATH_OUTPUT_RELATIVE / 'manifest_versions'

COL_SUBJECT_IMAGING = 'Subject ID'
COL_VISIT_IMAGING = 'Visit'
COL_GROUP_IMAGING = 'Research Group'
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
DATATYPES = [DATATYPE_ANAT, DATATYPE_DWI, DATATYPE_FUNC]

COL_SUBJECT_TABULAR = 'PATNO'
COL_VISIT_TABULAR = 'EVENT_ID'
COL_GROUP_TABULAR = 'COHORT_DEFINITION'
VISIT_SESSION_MAP = {
    'BL': '1',
    'V04': '5',
    'V06': '7',
    'V08': '9',
    'V10': '11',
    'PW': '30', 
    'ST': '21',
    'U02': '91',
    'U01': '90',
    'SC': '0',
    'R01': '3', # Month 6
}

# manifest filename and columns
FNAME_MANIFEST = 'mr_proc_manifest.csv' # symlink
PATTERN_MANIFEST_BACKUP = 'mr_proc_manifest-{}.csv'
COL_SUBJECT_MANIFEST = 'participant_id'
COL_VISIT_MANIFEST = 'visit'
COL_SESSION_MANIFEST = 'session'
COL_DATATYPE_MANIFEST = 'datatype'
COLS_MANIFEST = [COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, 
                 COL_SESSION_MANIFEST, COL_DATATYPE_MANIFEST]

# BIDS format
PATTERN_BIDS_SESSION = 'ses-{}'

# global config keys
GLOBAL_CONFIG_DATASET_ROOT = 'DATASET_ROOT'
GLOBAL_CONFIG_SESSIONS = 'SESSIONS'

# flags
FLAG_REGENERATE = '--regenerate'

def run(global_config_file, imaging_filename, tabular_filenames, group_filename, dpath_backups_relative, regenerate):

    # parse global config
    with open(global_config_file) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])

    validate_visit_session_map(global_config)

    # generate filepaths
    dpath_input = dpath_dataset / DPATH_INPUT_RELATIVE
    fpath_imaging = dpath_input / imaging_filename
    fpaths_tabular = [dpath_input / tabular_filename for tabular_filename in tabular_filenames]
    fpath_group = dpath_input / group_filename
    fpath_descriptions = Path(__file__).parent / FNAME_DESCRIPTIONS
    fpath_manifest_symlink = dpath_dataset / DPATH_OUTPUT_RELATIVE / FNAME_MANIFEST
    fname_manifest_backup = PATTERN_MANIFEST_BACKUP.format(
        datetime.datetime.now().strftime('%Y%m%d_%H%M')
    )
    fpath_manifest_backup = dpath_dataset / dpath_backups_relative / fname_manifest_backup
    
    # create backup directory if needed
    fpath_manifest_backup.parent.mkdir(parents=True, exist_ok=True)

    # load data dfs and heuristics json
    df_imaging = pd.read_csv(fpath_imaging, dtype=str)
    df_group = pd.read_csv(fpath_group, dtype=str)
    df_tabular = None
    for fpath_tabular in fpaths_tabular:
        df_tabular_tmp = pd.read_csv(fpath_tabular, dtype=str)
        if not len({COL_SUBJECT_TABULAR, COL_VISIT_TABULAR} - set(df_tabular_tmp.columns)) == 0:
            raise RuntimeError(f'Tabular file {fpath_tabular} does not contain required columns')
        df_tabular: pd.DataFrame = pd.concat([df_tabular, df_tabular_tmp])
    df_tabular = df_tabular.drop_duplicates([COL_SUBJECT_TABULAR, COL_VISIT_TABULAR])

    with fpath_descriptions.open('r') as file_descriptions:
        datatype_descriptions_map: dict = json.load(file_descriptions)
    
    # reverse the mapping
    description_datatype_map = {}
    for datatype, descriptions in datatype_descriptions_map.items():
        if datatype not in DATATYPES:
            continue
        for description in descriptions:
            if description in description_datatype_map:
                print(f'\nDescription {description} has more than one associated datatype')
            description_datatype_map[description] = datatype

    # ===== format imaging data =====

    # rename columns
    df_imaging = df_imaging.rename(columns={
        COL_SUBJECT_IMAGING: COL_SUBJECT_MANIFEST,
        COL_VISIT_IMAGING: COL_VISIT_MANIFEST,
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

    # map visits to sessions
    missing_session_mappings = set(df_imaging[COL_VISIT_MANIFEST]) - set(VISIT_SESSION_MAP.keys())
    if len(missing_session_mappings) > 0:
        print(f'\nMissing mapping(s) in VISIT_SESSION_MAP: {missing_session_mappings}')
    df_imaging[COL_SESSION_MANIFEST] = df_imaging[COL_VISIT_MANIFEST].map(VISIT_SESSION_MAP)

    # map group to tabular data naming scheme
    try:
        df_imaging[COL_GROUP_TABULAR] = df_imaging[COL_GROUP_IMAGING].apply(
            lambda group: GROUP_IMAGING_MAP[group]
        )
    except KeyError as ex:
        raise RuntimeError(
            f'Found group without mapping in GROUP_IMAGING_MAP: {ex.args[0]}')

    # ===== format tabular data =====

    # rename columns
    df_tabular = df_tabular.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST, 
        COL_VISIT_TABULAR: COL_VISIT_MANIFEST,
    })
    df_group = df_group.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST,
    })

    # add group info to tabular dataframe
    df_tabular = df_tabular.merge(df_group[[COL_SUBJECT_MANIFEST, COL_GROUP_TABULAR]], on=COL_SUBJECT_MANIFEST, how='left')
    if df_tabular[COL_GROUP_TABULAR].isna().any():
        
        df_tabular_missing_group = df_tabular.loc[
            df_tabular[COL_GROUP_TABULAR].isna(),
            COL_SUBJECT_MANIFEST,
        ]
        print(
            '\nSome subjects in tabular data do not belong to any research group'
            f'\n{df_tabular_missing_group}'
        )

        # try to find group in imaging dataframe
        for idx, subject in df_tabular_missing_group.items():

            group = df_imaging.loc[
                df_imaging[COL_SUBJECT_MANIFEST] == subject,
                COL_GROUP_TABULAR,
            ].drop_duplicates()

            try:
                group = group.item()
            except ValueError:
                continue

            df_tabular.loc[idx, COL_GROUP_TABULAR] = group

        if df_tabular[COL_GROUP_TABULAR].isna().any():
            print(
                'Did not successfully fill in missing group values using imaging data'
                f'\n{df_tabular.loc[df_tabular[COL_GROUP_TABULAR].isna()]}')

        else:
            print('\nSuccessfully filled in missing group values using imaging data')

    # ===== process imaging data =====

    print(
        '\nProcessing imaging data...'
        f'\tShape: {df_imaging.shape}'
        '\nSession counts:'
        f'\n{df_imaging[COL_SESSION_MANIFEST].value_counts(dropna=False)}\n'
    )

    # check if all expected sessions are present
    diff_sessions = set(global_config[GLOBAL_CONFIG_SESSIONS]) - set(df_imaging[COL_SESSION_MANIFEST])
    if len(diff_sessions) != 0:
        print(f'Did not encounter all sessions listed in global_config. Missing: {diff_sessions}')

    # only keep sessions that are listed in global_config
    n_img_before_session_drop = df_imaging.shape[0]
    df_imaging = df_imaging.loc[df_imaging[COL_SESSION_MANIFEST].isin(global_config[GLOBAL_CONFIG_SESSIONS])]
    print(
        f'\n\tDropped {n_img_before_session_drop - df_imaging.shape[0]} imaging entries'
        f' because the session was not in {global_config[GLOBAL_CONFIG_SESSIONS]}'
        '\nCohort composition:'
        f'\n{df_imaging[COL_GROUP_TABULAR].value_counts(dropna=False)}\n'
    )

    # check if all expected groups are present
    diff_groups = set(GROUPS_KEEP) - set(df_imaging[COL_GROUP_TABULAR])
    if len(diff_groups) != 0:
        print(f'Did not encounter all groups listed in GROUPS_KEEP. Missing: {diff_groups}')

    # only keep subjects in certain groups
    n_img_before_subject_drop = df_imaging.shape[0]
    df_imaging = df_imaging.loc[df_imaging[COL_GROUP_TABULAR].isin(GROUPS_KEEP)]
    print(
        f'\n\tDropped {n_img_before_subject_drop - df_imaging.shape[0]} imaging entries'
        f' because the subject\'s research group was not in {GROUPS_KEEP}'
    )

    # create imaging datatype availability lists
    seen_datatypes = set()
    df_imaging = df_imaging.groupby([COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, COL_SESSION_MANIFEST])[COL_DATATYPE_MANIFEST].aggregate(
        lambda descriptions: get_datatype_list(descriptions, description_datatype_map, seen=seen_datatypes)
    )
    df_imaging = df_imaging.reset_index()
    print(f'\n\tFinal imaging dataframe shape: {df_imaging.shape}')

    # check if all expected datatypes are present
    diff_datatypes = set(DATATYPES) - seen_datatypes
    if len(diff_datatypes) != 0:
        print(f'Did not encounter all datatypes in datatype_descriptions_map. Missing: {diff_datatypes}')
    
    print(
        '\nProcessing tabular data...'
        f'\tShape: {df_tabular.shape}'
        '\nCohort composition:'
        f'\n{df_tabular[COL_GROUP_TABULAR].value_counts(dropna=False)}\n'
    )

    # only keep subjects in certain groups
    n_tab_before_subject_drop = df_tabular.shape[0]
    df_tabular = df_tabular.loc[df_tabular[COL_GROUP_TABULAR].isin(GROUPS_KEEP)]
    print(
        f'\n\tDropped {n_tab_before_subject_drop - df_tabular.shape[0]} tabular entries'
        f' because the subject\'s research group was not in {GROUPS_KEEP}\n'
    )

    # merge on subject and visit
    key_merge = '_merge'
    df_manifest = df_tabular.merge(df_imaging, how='outer', 
                                   on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST],
                                   indicator=key_merge)
    
    # warning if missing tabular information
    df_imaging_without_tabular = df_manifest.loc[df_manifest[key_merge] == 'right_only']
    if len(df_imaging_without_tabular) > 0:
        print(
            '\nSome imaging entries have no corresponding tabular information'
            f'{df_imaging_without_tabular}\n'
        )

    # replace NA datatype by empty list
    df_manifest[COL_DATATYPE_MANIFEST] = df_manifest[COL_DATATYPE_MANIFEST].apply(
        lambda datatype: datatype if isinstance(datatype, list) else []
    )

    # convert session to BIDS format
    df_manifest[COL_SESSION_MANIFEST] = df_manifest[COL_SESSION_MANIFEST].apply(
        lambda session: session if pd.isna(session) else PATTERN_BIDS_SESSION.format(session)
    )

    # populate other columns and select/reorder columns used in manifest
    for col in COLS_MANIFEST:
        if not (col in df_manifest.columns):
            df_manifest[col] = np.nan
    df_manifest = df_manifest[COLS_MANIFEST]

    # sort
    df_manifest = df_manifest.sort_values([COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]).reset_index(drop=True)

    print(
        '\nCreated manifest:'
        f'\n{df_manifest}'
    )

    if fpath_manifest_symlink.exists():

        df_manifest_old = pd.read_csv(
            fpath_manifest_symlink, 
            dtype={COL_SUBJECT_MANIFEST: str},
            converters={COL_DATATYPE_MANIFEST: pd.eval}
        )

        if df_manifest.equals(df_manifest_old):
            print(f'\nFound an existing manifest with exactly the same information, exiting')
            return
        
        fpath_manifest_symlink.unlink()

    df_manifest.to_csv(fpath_manifest_backup, index=False, header=True)
    print(f'File written to: {fpath_manifest_backup}')
    fpath_manifest_symlink.symlink_to(fpath_manifest_backup)
    print(f'Created symlink: {fpath_manifest_symlink} -> {fpath_manifest_backup}')

    # set file permissions
    os.chmod(fpath_manifest_backup, 0o664)

def validate_visit_session_map(global_config):
    if len(set(global_config[GLOBAL_CONFIG_SESSIONS]) - set(VISIT_SESSION_MAP.values())) > 0:
        raise ValueError(
            f'Invalid VISIT_SESSION_MAP: {VISIT_SESSION_MAP}. Must have an entry'
            f' for each session in global_config: {global_config[GLOBAL_CONFIG_SESSIONS]}')

def get_datatype_list(descriptions: pd.Series, description_datatype_map, seen=None):

    datatypes = descriptions.map(description_datatype_map)
    datatypes = datatypes.loc[~datatypes.isna()]
    datatypes = datatypes.drop_duplicates().sort_values().to_list()

    if isinstance(seen, set):
        seen.update(datatypes)

    return datatypes

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Script to generate manifest file for PPMI dataset.
    Requires an imaging data availability info file that can be downloaded from 
    the LONI IDA, the PPMI participant status info files, as well as at least 
    one PPMI tabular file with subject and visit columns. 
    All these files should be in <DATASET_ROOT>/{DPATH_INPUT_RELATIVE}.
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your mr_proc dataset (required)')
    parser.add_argument(
        '--imaging_filename', type=str, default=DEFAULT_IMAGING_FILENAME,
        help=('name of file containing imaging data availability info, with columns'
              f' "{COL_SUBJECT_IMAGING}", "{COL_VISIT_IMAGING}", "{COL_GROUP_IMAGING}", and "{COL_DESCRIPTION_IMAGING}"'
              f' (default: {DEFAULT_IMAGING_FILENAME})'))
    parser.add_argument(
        '--tabular_filenames', type=str, nargs='+', default=DEFAULT_TABULAR_FILENAMES,
        help=('name of files containing tabular data availability info, with columns'
              f' "{COL_SUBJECT_TABULAR}" and "{COL_VISIT_TABULAR}"'
              f' (default: {DEFAULT_TABULAR_FILENAMES if len(DEFAULT_TABULAR_FILENAMES) <= 5 else f"{len(DEFAULT_TABULAR_FILENAMES)} files"})'))
    parser.add_argument(
        '--group_filename', type=str, default=DEFAULT_GROUP_FILENAME,
        help=('name of file containing participant group info, with columns'
              f' "{COL_SUBJECT_TABULAR}" and "{COL_GROUP_TABULAR}"'
              f' (default: {DEFAULT_GROUP_FILENAME})'))
    parser.add_argument(
        '--backups', type=str, default=DEFAULT_DPATH_BACKUPS_RELATIVE,
        help=('path to backups directory for storing different versions of the'
              f' manifest, relative to <DATASET_ROOT> (default: {DEFAULT_DPATH_BACKUPS_RELATIVE})')
    )
    parser.add_argument(
        FLAG_REGENERATE, action='store_true',
        help=('regenerate entire manifest'
              ' (default: only append rows for new subjects/sessions)'),
    )
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config
    imaging_filename = args.imaging_filename
    tabular_filenames = args.tabular_filenames
    group_filename = args.group_filename
    dpath_backups = args.backups
    regenerate = getattr(args, FLAG_REGENERATE.lstrip('-'))

    run(global_config_file, imaging_filename, tabular_filenames, group_filename, 
        dpath_backups_relative=dpath_backups, regenerate=regenerate)
