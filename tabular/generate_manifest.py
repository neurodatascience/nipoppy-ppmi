#!/usr/bin/env python

import argparse
import datetime
import json
import shutil
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from tabular.filter_image_descriptions import COL_DESCRIPTION as COL_DESCRIPTION_IMAGING
from tabular.filter_image_descriptions import FNAME_DESCRIPTIONS, DATATYPE_ANAT, DATATYPE_DWI, DATATYPE_FUNC, get_all_descriptions
from tabular.ppmi_utils import get_tabular_info, COL_GROUP_TABULAR, COL_SUBJECT_TABULAR, COL_VISIT_TABULAR
from workflow.utils import (
    COL_BIDS_ID_MANIFEST,
    COL_DATATYPE_MANIFEST,
    COL_SESSION_MANIFEST,
    COL_SUBJECT_MANIFEST,
    COL_VISIT_MANIFEST,
    COLS_MANIFEST,
    DNAME_BACKUPS_MANIFEST, 
    FNAME_MANIFEST,
    load_manifest,
    participant_id_to_bids_id,
    save_backup, 
    session_id_to_bids_session,
)

# subject groups to keep
GROUPS_KEEP = ['Parkinson\'s Disease', 'Prodromal', 'Healthy Control', 'SWEDD']

# paths relative to DATASET_ROOT
# TODO refactor global configs
DPATH_TABULAR_RELATIVE = Path('tabular')
DPATH_ASSESSMENTS_RELATIVE = DPATH_TABULAR_RELATIVE / 'assessments'
DPATH_DEMOGRAPHICS_RELATIVE = DPATH_TABULAR_RELATIVE / 'demographics'
DPATH_OTHER_RELATIVE = DPATH_TABULAR_RELATIVE / 'other'
DPATH_RELEASES_RELATIVE = Path('releases')
DPATH_OUTPUT_RELATIVE = DPATH_TABULAR_RELATIVE

# TODO move to ppmi_utils.py
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

# global config keys
GLOBAL_CONFIG_DATASET_ROOT = 'DATASET_ROOT'
GLOBAL_CONFIG_SESSIONS = 'SESSIONS'
GLOBAL_CONFIG_VISITS = 'VISITS'
GLOBAL_CONFIG_TABULAR = 'TABULAR'

# flags
FLAG_REGENERATE = '--regenerate'

def run(global_config_file: str, regenerate: bool, make_release: bool):

    # parse global config
    with open(global_config_file) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])
    visits = global_config[GLOBAL_CONFIG_VISITS]

    # generate filepaths
    dpath_demographics = dpath_dataset / DPATH_DEMOGRAPHICS_RELATIVE
    dpath_assessments = dpath_dataset / DPATH_ASSESSMENTS_RELATIVE
    dpath_other = dpath_dataset / DPATH_OTHER_RELATIVE
    fpaths_demographics = list({dpath_demographics / file_info['FILENAME'] for file_info in global_config[GLOBAL_CONFIG_TABULAR]['DEMOGRAPHICS'].values()})
    fpaths_assessments = list({dpath_assessments / file_info['FILENAME'] for file_info in global_config[GLOBAL_CONFIG_TABULAR]['ASSESSMENTS'].values()})
    fpath_imaging = dpath_other / global_config[GLOBAL_CONFIG_TABULAR]['OTHER']['IMAGING_INFO']['FILENAME']
    fpath_group = dpath_demographics / global_config[GLOBAL_CONFIG_TABULAR]['DEMOGRAPHICS']['COHORT_DEFINITION']['FILENAME']
    fpath_descriptions = Path(__file__).parent / FNAME_DESCRIPTIONS
    fpath_manifest_symlink = dpath_dataset / DPATH_OUTPUT_RELATIVE / FNAME_MANIFEST
    dpaths_include_in_release = [dpath_demographics, dpath_assessments, dpath_other]

    for fpath in [fpath_imaging, *fpaths_demographics, *fpaths_assessments, fpath_group, fpath_descriptions]:
        if not fpath.exists():
            raise RuntimeError(f'File {fpath} does not exist')

    # load old manifest if it exists
    if fpath_manifest_symlink.exists():
        df_manifest_old = load_manifest(fpath_manifest_symlink)
    else:
        df_manifest_old = None

    # load data dfs and heuristics json
    df_imaging = load_and_process_df_imaging(fpath_imaging)
    df_group = pd.read_csv(fpath_group, dtype=str)

    # this is a hack to get static and non-static 
    # data combining demographic and assessment data
    tabular_info_dict = {}
    for tabular_key, dname_parent in zip(['DEMOGRAPHICS', 'ASSESSMENTS'], ['demographics', 'assessments']):
        for col_name, info in global_config['TABULAR'][tabular_key].items():
            info['FILENAME'] = Path(dname_parent) / info['FILENAME']
            if col_name in tabular_info_dict:
                raise RuntimeError(f'Tabular column name {col_name} is duplicated in the global configs file')
            tabular_info_dict[col_name] = info

    df_static, df_nonstatic = get_tabular_info(
        tabular_info_dict,
        dpath_dataset / 'tabular',
        visits=visits,
    )

    with fpath_descriptions.open('r') as file_descriptions:
        datatype_descriptions_map: dict = json.load(file_descriptions)
    
    # reverse the mapping
    description_datatype_map = {}
    for datatype in DATATYPES:
        descriptions = get_all_descriptions(datatype_descriptions_map[datatype])
        for description in descriptions:
            if description in description_datatype_map:
                warnings.warn(f'\nDescription {description} has more than one associated datatype, using "{description_datatype_map[description]}"\n')
            else:
                description_datatype_map[description] = datatype

    # ===== format tabular data =====

    # rename columns
    df_nonstatic = df_nonstatic.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST, 
        COL_VISIT_TABULAR: COL_VISIT_MANIFEST,
    })
    df_group = df_group.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST,
    })
    df_static = df_static.rename(columns={
        COL_SUBJECT_TABULAR: COL_SUBJECT_MANIFEST,
    })

    # add group info to tabular dataframe
    df_nonstatic = df_nonstatic.merge(df_group[[COL_SUBJECT_MANIFEST, COL_GROUP_TABULAR]], on=COL_SUBJECT_MANIFEST, how='left')
    if df_nonstatic[COL_GROUP_TABULAR].isna().any():
        
        df_tabular_missing_group = df_nonstatic.loc[
            df_nonstatic[COL_GROUP_TABULAR].isna(),
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

            df_nonstatic.loc[idx, COL_GROUP_TABULAR] = group

        if df_nonstatic[COL_GROUP_TABULAR].isna().any():
            warnings.warn(
                '\nDid not successfully fill in missing group values using imaging data'
                f'\n{df_nonstatic.loc[df_nonstatic[COL_GROUP_TABULAR].isna()]}')

        else:
            print('Successfully filled in missing group values using imaging data')

    # ===== process imaging data =====

    print(f'\nProcessing imaging data...\tShape: {df_imaging.shape}')
    print('\nSession counts:'
        f'\n{df_imaging[COL_SESSION_MANIFEST].value_counts(dropna=False)}'
    )

    # check if all expected sessions are present
    diff_sessions = set(global_config[GLOBAL_CONFIG_SESSIONS]) - set(df_imaging[COL_SESSION_MANIFEST])
    if len(diff_sessions) != 0:
        warnings.warn(f'\nDid not encounter all sessions listed in global_config. Missing: {diff_sessions}')

    # only keep sessions that are listed in global_config
    n_img_before_session_drop = df_imaging.shape[0]
    df_imaging = df_imaging.loc[df_imaging[COL_SESSION_MANIFEST].isin(global_config[GLOBAL_CONFIG_SESSIONS])]
    print(
        f'\nDropped {n_img_before_session_drop - df_imaging.shape[0]} imaging entries'
        f' because the session was not in {global_config[GLOBAL_CONFIG_SESSIONS]}'
    )
    print('\nCohort composition:'
        f'\n{df_imaging[COL_GROUP_TABULAR].value_counts(dropna=False)}'
    )

    # check if all expected groups are present
    diff_groups = set(GROUPS_KEEP) - set(df_imaging[COL_GROUP_TABULAR])
    if len(diff_groups) != 0:
        warnings.warn(f'\nDid not encounter all groups listed in GROUPS_KEEP. Missing: {diff_groups}')

    # only keep subjects in certain groups
    n_img_before_subject_drop = df_imaging.shape[0]
    df_imaging = df_imaging.loc[df_imaging[COL_GROUP_TABULAR].isin(GROUPS_KEEP)]
    print(
        f'\nDropped {n_img_before_subject_drop - df_imaging.shape[0]} imaging entries'
        f' because the subject\'s research group was not in {GROUPS_KEEP}'
    )

    # create imaging datatype availability lists
    seen_datatypes = set()
    df_imaging = df_imaging.groupby([COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST, COL_SESSION_MANIFEST])[COL_DATATYPE_MANIFEST].aggregate(
        lambda descriptions: get_datatype_list(descriptions, description_datatype_map, seen=seen_datatypes)
    )
    df_imaging = df_imaging.reset_index()
    print(f'\nFinal imaging dataframe shape: {df_imaging.shape}')

    # check if all expected datatypes are present
    diff_datatypes = set(DATATYPES) - seen_datatypes
    if len(diff_datatypes) != 0:
        warnings.warn(f'\nDid not encounter all datatypes in datatype_descriptions_map. Missing: {diff_datatypes}')
    
    print(
        '\nProcessing tabular data...'
        f'\tShape: {df_nonstatic.shape}'
    )
    print('\nCohort composition:'
        f'\n{df_nonstatic[COL_GROUP_TABULAR].value_counts(dropna=False)}\n'
    )

    # only keep subjects in certain groups
    n_tab_before_subject_drop = df_nonstatic.shape[0]
    df_nonstatic = df_nonstatic.loc[df_nonstatic[COL_GROUP_TABULAR].isin(GROUPS_KEEP)]
    print(
        f'\nDropped {n_tab_before_subject_drop - df_nonstatic.shape[0]} tabular entries'
        f' because the subject\'s research group was not in {GROUPS_KEEP}\n'
    )

    # merge on subject and visit
    key_merge = '_merge'
    df_manifest = df_nonstatic.merge(df_imaging, how='outer', 
                                   on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST],
                                   indicator=key_merge)
    
    # warning if missing tabular information
    subjects_without_demographic = set(df_manifest[COL_SUBJECT_MANIFEST]) - set(df_nonstatic[COL_SUBJECT_MANIFEST])
    if len(subjects_without_demographic) > 0:
        print(
            '\nSome subjects have imaging data but no demographic information'
            f'\n{subjects_without_demographic}, dropping them from the manifest'
        )
    df_manifest = df_manifest.loc[~df_manifest[COL_SUBJECT_MANIFEST].isin(subjects_without_demographic)]

    # replace NA datatype by empty list
    df_manifest[COL_DATATYPE_MANIFEST] = df_manifest[COL_DATATYPE_MANIFEST].apply(
        lambda datatype: datatype if isinstance(datatype, list) else []
    )

    # convert session to BIDS format
    with_imaging = ~df_manifest[COL_SESSION_MANIFEST].isna()
    df_manifest.loc[with_imaging, COL_SESSION_MANIFEST] = df_manifest.loc[with_imaging, COL_SESSION_MANIFEST].apply(
        session_id_to_bids_session,
    )

    # convert subject ID to BIDS format
    df_manifest.loc[with_imaging, COL_BIDS_ID_MANIFEST] = df_manifest.loc[with_imaging, COL_SUBJECT_MANIFEST].apply(
        participant_id_to_bids_id,
    )

    # populate other columns
    for col in COLS_MANIFEST:
        if not (col in df_manifest.columns):
            df_manifest[col] = np.nan

    # only keep new subject/session pairs
    # otherwise we build/rebuild the manifest from scratch
    if (not regenerate) and (df_manifest_old is not None):

        subject_session_pairs_old = pd.Index(zip(
            df_manifest_old[COL_SUBJECT_MANIFEST],
            df_manifest_old[COL_SESSION_MANIFEST],
        ))

        df_manifest = df_manifest.set_index([COL_SUBJECT_MANIFEST, COL_SESSION_MANIFEST])

        # error if new manifest loses subject-session pairs
        df_manifest_deleted_rows = df_manifest_old.loc[~subject_session_pairs_old.isin(df_manifest.index)]
        if len(df_manifest_deleted_rows) > 0:
            raise RuntimeError(
                'Some of the subject/session pairs in the old manifest do not'
                ' seem to exist anymore:'
                f'\n{df_manifest_deleted_rows}'
                f'\nUse {FLAG_REGENERATE} to fully regenerate the manifest')

        df_manifest_new_rows = df_manifest.loc[~df_manifest.index.isin(subject_session_pairs_old)]
        df_manifest_new_rows = df_manifest_new_rows.reset_index()[COLS_MANIFEST]
        df_manifest = pd.concat([df_manifest_old, df_manifest_new_rows], axis='index')
        print(f'\nAdded {len(df_manifest_new_rows)} rows to existing manifest')

    # reorder columns and sort
    df_manifest = df_manifest[COLS_MANIFEST]
    df_manifest = df_manifest.sort_values(
        COL_VISIT_MANIFEST, 
        key=(lambda visits: visits.apply(global_config[GLOBAL_CONFIG_VISITS].index)),
    )
    df_manifest = df_manifest.sort_values(COL_SUBJECT_MANIFEST, kind='stable').reset_index(drop=True)

    # do not write file if there are no changes from previous manifest
    if df_manifest_old is not None:
        if df_manifest.equals(df_manifest_old):
            print(f'\nNo change from existing manifest. Will not write new manifest.')
            if make_release:
                make_new_release(dpath_dataset, dpaths_include_in_release)
            return
        fpath_manifest_symlink.unlink()

    print(
        '\nCreated manifest:'
        f'\n{df_manifest}'
    )

    save_backup(df_manifest, fpath_manifest_symlink, DNAME_BACKUPS_MANIFEST)

    if make_release:
        make_new_release(dpath_dataset, dpaths_include_in_release)

def load_and_process_df_imaging(fpath_imaging):

    # load
    df_imaging = pd.read_csv(fpath_imaging, dtype=str)

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

def get_datatype_list(descriptions: pd.Series, description_datatype_map, seen=None):

    datatypes = descriptions.map(description_datatype_map)
    datatypes = datatypes.loc[~datatypes.isna()]
    datatypes = datatypes.drop_duplicates().sort_values().to_list()

    if isinstance(seen, set):
        seen.update(datatypes)

    return datatypes

def make_new_release(dpath_dataset: Path, dpaths_include):

    def ignore_func(dpath, fnames):
        if Path(dpath) in dpaths_include:
            return fnames
        else:
            return []
        
    dpaths_include = [Path(dpath) for dpath in dpaths_include]

    date_str = datetime.datetime.now().strftime('%Y_%m_%d')
    dpath_source = dpath_dataset / DPATH_TABULAR_RELATIVE
    dpath_target = dpath_dataset / DPATH_RELEASES_RELATIVE / date_str

    if dpath_target.exists():
        raise FileExistsError(f'Release directory already exists: {dpath_target}')
    
    shutil.copytree(dpath_source, dpath_target, symlinks=True, ignore=ignore_func)
    print(f'\nNew release created: {dpath_target}')

def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
    return '%s:%s: %s: %s\n' % (filename, lineno, category.__name__, message)

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Script to generate manifest file for PPMI dataset.
    Requires an imaging data availability info file that can be downloaded from 
    the LONI IDA, as well as the demographic information CSV file from the PPMI website.
    The name of these files should be specified in the global config file.
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your mr_proc dataset (required)')
    parser.add_argument(
        FLAG_REGENERATE, action='store_true',
        help=('regenerate entire manifest'
              ' (default: only append rows for new subjects/sessions)'),
    )
    parser.add_argument(
        '--make_release', action='store_true',
        help=(f'copy <DATASET_ROOT>/{DPATH_TABULAR_RELATIVE} to a'
              f' release directory in <DATASET_ROOT>/{DPATH_RELEASES_RELATIVE}')
    )
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config
    make_release = args.make_release
    regenerate = getattr(args, FLAG_REGENERATE.lstrip('-'))

    warnings.formatwarning = warning_on_one_line

    run(global_config_file, regenerate=regenerate, make_release=make_release)
