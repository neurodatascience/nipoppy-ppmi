#!/usr/bin/env python
import argparse
import json
from pathlib import Path

import pandas as pd

from nipoppy.workflow.utils import (
    COL_BIDS_ID_MANIFEST,
    COL_SESSION_MANIFEST,
    COL_SUBJECT_MANIFEST,
    COL_VISIT_MANIFEST,
    load_manifest,
    save_backup,
    participant_id_to_bids_id,
    session_id_to_bids_session,
)
from nipoppy.workflow.ppmi_utils import get_tabular_info_and_merge, COL_SUBJECT_TABULAR, COL_VISIT_TABULAR

# TODO import from nipoppy.workflow.utils (nipoppy repo)
FPATH_ASSESSMENTS_RELATIVE = Path('tabular/assessments/assessments.csv')
FPATH_BAGEL_RELATIVE = Path('tabular/bagel.csv')
FPATH_DEMOGRAPHICS_RELATIVE = Path('tabular/demographics/demographics.csv')
DNAME_BACKUPS_ASSESSMENTS = '.assessments'
DNAME_BACKUPS_DEMOGRAPHICS = '.demographics'
DNAME_BACKUPS_BAGEL = '.bagels'

FPATH_DASH_BAGEL_RELATIVE = Path('tabular/dashboard_bagel.csv')
DNAME_BACKUPS_DASH_BAGEL = '.dashboard_bagels'
DASH_BAGEL_ID_COLS = [COL_BIDS_ID_MANIFEST, COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]
DASH_BAGEL_VAR_NAME = 'assessment_name'
DASH_BAGEL_VAR_VALUE = 'assessment_score'

COL_UPDRS3 = 'NP3TOT'
COL_AGE = 'AGE_AT_VISIT'
COL_EDUCATION = 'EDUCYRS'
COL_UPSIT = 'UPSIT_PRCNTGE'

def loading_func(df):
    if COL_UPDRS3 in df.columns:
        print(f'Filtering {COL_UPDRS3}')
        df = updrs3_on_off_splitter(df)
    if COL_AGE in df.columns:
        print(f'Filtering {COL_AGE}')
        df = age_filter(df)
    if COL_EDUCATION in df.columns:
        print(f'Filtering {COL_EDUCATION}')
        df = education_filter(df)
    if COL_UPSIT in df.columns:
        print(f'Filtering {COL_UPSIT}')
        df = upsit_filter(df)
    return df

def updrs3_on_off_splitter(df: pd.DataFrame):

    COL_OFF = f'{COL_UPDRS3}_OFF'
    COL_ON = f'{COL_UPDRS3}_ON'
    data_new_df = []
    cols = [COL_SUBJECT_TABULAR, COL_VISIT_TABULAR, 'PDSTATE', 'PAG_NAME', 'PDTRTMNT', COL_UPDRS3]
    for subject, session, pd_state, page, pd_treatment, updrs3 in df[cols].itertuples(index=False):
        
        target_col = COL_OFF
        if pd_state == 'ON':
            target_col = COL_ON
        elif pd_treatment != '0' and pd_state != 'OFF':
            if page == 'NUPDR3ON':
                target_col = COL_ON
            elif page != 'NUPDR3OF':
                if pd_treatment == '1':
                    target_col = COL_ON

        data_new_df.append({
            COL_SUBJECT_TABULAR: subject,
            COL_VISIT_TABULAR: session,
            target_col: float(updrs3),
        })

    df_on_off = pd.DataFrame(data_new_df).groupby([COL_SUBJECT_TABULAR, COL_VISIT_TABULAR]).max().reset_index()
    return df_on_off

def _find_duplicates(df: pd.DataFrame, cols_index, col_value):
    groups = df.groupby(cols_index)[col_value]
    counts = groups.count()
    records_with_multiple_ages = counts[counts > 1].index.unique()
    df_no_duplicates = df.set_index(cols_index).drop(index=records_with_multiple_ages)
    return records_with_multiple_ages, groups, df_no_duplicates

def _subject_sort_key(series):
    try:
        return series.astype(int)
    except Exception:
        return series

def age_filter(df: pd.DataFrame):
    def visit_sort_key(visit):
        # custom sorting key so that the order is: SC, then BL, then the rest in numerical order
        if visit == 'SC':
            return -10
        elif visit == 'BL':
            return -5
        else:
            return int(''.join([c for c in visit if c.isdigit()]))
    
    def visit_is_before_or_same(visit1, visit2):
        return visit_sort_key(visit1) <= visit_sort_key(visit2)
    
    df[COL_AGE] = df[COL_AGE].astype(float)

    # find subjects with multiple age entries for the same visit
    records_with_multiple_ages, groups, df_no_duplicates = _find_duplicates(df, [COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], COL_AGE)
    for record_to_fix in records_with_multiple_ages:
        # reduce duplicate ages into a single age by dropping "bad" ages and 
        # taking the mean of the ages not marked as "bad"
        # "bad" ages are those that are greater than (non-duplicated) ages at later visits
        subject, session = record_to_fix
        duplicate_ages = groups.get_group(record_to_fix)
        other_sessions: pd.DataFrame = df.loc[(df[COL_SUBJECT_TABULAR] == subject) & (df[COL_VISIT_TABULAR] != session)]
        bad_ages = []
        for duplicate_age in duplicate_ages:
            for _session, _age in other_sessions[[COL_VISIT_TABULAR, COL_AGE]].itertuples(index=False):
                if visit_is_before_or_same(session, _session) and duplicate_age >= _age:
                    bad_ages.append(duplicate_age)
        final_age = duplicate_ages[~duplicate_ages.isin(bad_ages)].mean()

        df_no_duplicates.loc[record_to_fix, COL_AGE] = final_age

    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(by=[COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], key=_subject_sort_key)
    return df_no_duplicates

def education_filter(df: pd.DataFrame):
    # education is a "static metric", meaning that only one value is expected for 
    # each participant. However, there are participants with more than one entry 
    # in that case we take the mean of the entries
    df[COL_EDUCATION] = df[COL_EDUCATION].astype(float)

    # first we drop the duplicates where the education year is the same
    df = df.drop_duplicates([COL_SUBJECT_TABULAR, COL_EDUCATION])

    # we also drop rows with missing values
    df = df.dropna(axis='index', subset=COL_EDUCATION)

    subjects_with_multiple_edu, groups, df_no_duplicates = _find_duplicates(df, [COL_SUBJECT_TABULAR], COL_EDUCATION)
    for subject_to_fix in subjects_with_multiple_edu:
        duplicate_edus = groups.get_group(subject_to_fix)
        df_no_duplicates.loc[subject_to_fix, COL_EDUCATION] = duplicate_edus.mean()
    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(by=COL_SUBJECT_TABULAR, key=_subject_sort_key)
    
    return df_no_duplicates

def upsit_filter(df: pd.DataFrame):
    # take the mean UPSIT score
    df[COL_UPSIT] = df[COL_UPSIT].astype(float)

    # we also drop rows with missing values
    df = df.dropna(axis='index', subset=COL_UPSIT)

    subjects_with_multiple_edu, groups, df_no_duplicates = _find_duplicates(df, [COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], COL_UPSIT)
    for subject_to_fix in subjects_with_multiple_edu:
        duplicate_edus = groups.get_group(subject_to_fix)
        df_no_duplicates.loc[subject_to_fix, COL_UPSIT] = duplicate_edus.mean()
    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(by=[COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], key=_subject_sort_key)
    
    return df_no_duplicates


def run(fpath_global_config):

    # load the global config
    with Path(fpath_global_config).open('r') as file_global_config:
        global_config = json.load(file_global_config)
    dataset_root = Path(global_config['DATASET_ROOT'])
    fpath_demographics = dataset_root / FPATH_DEMOGRAPHICS_RELATIVE
    fpath_assessments = dataset_root / FPATH_ASSESSMENTS_RELATIVE
    fpath_bagel = dataset_root / FPATH_BAGEL_RELATIVE
    fpath_dash_bagel = dataset_root / FPATH_DASH_BAGEL_RELATIVE
    visits = global_config['VISITS']

    # load the manifest
    fpath_manifest = dataset_root / 'tabular' / 'manifest.csv'
    df_manifest = load_manifest(fpath_manifest)

    # only keep indexing cols
    df_manifest = df_manifest[[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST]]

    # combine demographics info
    df_demographics = process_tabular_and_save(
        global_config['TABULAR']['DEMOGRAPHICS'],
        dataset_root / 'tabular' / 'demographics',
        df_manifest,
        visits,
        fpath_demographics, 
        DNAME_BACKUPS_DEMOGRAPHICS, 
        'demographics',
        loading_func=loading_func,
    )

    # combine assessments info
    df_assessments = process_tabular_and_save(
        global_config['TABULAR']['ASSESSMENTS'],
        dataset_root / 'tabular' / 'assessments',
        df_manifest,
        visits,
        fpath_assessments,
        DNAME_BACKUPS_ASSESSMENTS,
        'assessments',
        loading_func=loading_func,
    )

    # combine everything into a single bagel
    df_bagel = df_demographics.merge(df_assessments, on=[COL_SUBJECT_MANIFEST, COL_VISIT_MANIFEST], how='outer')
    df_bagel = df_bagel.drop_duplicates().reset_index(drop=True)
    df_bagel.insert(1, COL_BIDS_ID_MANIFEST, df_bagel[COL_SUBJECT_MANIFEST].apply(participant_id_to_bids_id))
    print(f'\nGenerated bagel: {df_bagel.shape}')

    # check that the bagel has no duplicate entries
    idx_bagel = df_bagel.loc[:, [COL_BIDS_ID_MANIFEST, COL_VISIT_MANIFEST]].apply(lambda df: ' '.join(df.dropna().astype(str).values), axis=1)
    idx_bagel_counts = idx_bagel.value_counts()
    has_duplicates = idx_bagel_counts.loc[idx_bagel_counts > 1]
    if len(has_duplicates) > 0:
        df_bagel_duplicates = df_bagel.loc[idx_bagel.isin(has_duplicates.index)]
        df_bagel_duplicates.to_csv('bagel_duplicates.csv', index=False)
        raise RuntimeError(f'Bagel has duplicate entries (saved to bagel_duplicates.csv):\n{df_bagel_duplicates}')

    # save bagel
    if fpath_bagel.exists() and pd.read_csv(fpath_bagel, dtype=str).equals(df_bagel):
        print('No changes to bagel file. Will not write new file.')
    else:
        save_backup(df_bagel, fpath_bagel, DNAME_BACKUPS_BAGEL)

    # make and save dashboard bagel
    df_dash_bagel = pd.melt(df_bagel, id_vars=DASH_BAGEL_ID_COLS, var_name=DASH_BAGEL_VAR_NAME,value_name=DASH_BAGEL_VAR_VALUE)
    df_dash_bagel = df_dash_bagel.rename(columns={COL_VISIT_MANIFEST: COL_SESSION_MANIFEST})
    df_dash_bagel[COL_SESSION_MANIFEST] = df_dash_bagel[COL_SESSION_MANIFEST].apply(session_id_to_bids_session)
    save_backup(df_dash_bagel, fpath_dash_bagel, DNAME_BACKUPS_DASH_BAGEL)

def process_tabular_and_save(info_dict, dpath_parent, df_manifest, visits, fpath, dname_backups, tag, loading_func=None):
    df = get_tabular_info_and_merge(info_dict, dpath_parent, df_manifest=df_manifest, visits=visits, loading_func=loading_func)
    if Path(fpath).exists() and pd.read_csv(fpath, dtype=str).equals(df):
        print(f'No changes to {tag} file. Will not write new file.')
    else:
        save_backup(df, fpath, dname_backups)
    return df

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Process/aggregate tabular data and create a single bagel file for tracking tabular data availability.
    """

    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your nipoppy dataset (required)')
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config

    run(global_config_file)

