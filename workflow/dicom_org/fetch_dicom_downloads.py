#!/usr/bin/env python

import argparse
import glob
import json
from joblib import Parallel, delayed
from pathlib import Path

import pandas as pd

import tabular.filter_image_descriptions
import workflow.logger as my_logger
from tabular.filter_image_descriptions import (
    FNAME_DESCRIPTIONS, 
    DATATYPE_ANAT, 
    DATATYPE_DWI, 
    DATATYPE_FUNC,
    get_all_descriptions,
)
from tabular.generate_manifest import (
    COL_DESCRIPTION_IMAGING,
    COL_GROUP_TABULAR,
    COL_SUBJECT_IMAGING,
    COL_VISIT_IMAGING,
    GROUPS_KEEP,
    DEFAULT_IMAGING_FILENAME,
    GLOBAL_CONFIG_DATASET_ROOT,
    load_and_process_df_imaging
)
from workflow.utils import (
    COL_DATATYPE_MANIFEST,
    COL_DOWNLOAD_STATUS,
    COL_SESSION_MANIFEST,
    COL_SUBJECT_MANIFEST,
    DNAME_BACKUPS_STATUS,
    FNAME_MANIFEST,
    FNAME_STATUS,
    load_status,
    save_backup,
    session_id_to_bids_session,
)

# default command-line arguments
DEFAULT_DATATYPES = [DATATYPE_ANAT, DATATYPE_DWI]
DEFAULT_N_JOBS = 4
DEFAULT_CHUNK_SIZE = 1000

# imaging dataframe
COL_IMAGE_ID = 'Image ID'

DPATH_TABULAR_RELATIVE = Path('tabular')
DPATH_STUDY_DATA_RELATIVE = DPATH_TABULAR_RELATIVE / 'study_data'
DPATH_RAW_DICOM_RELATIVE = Path('scratch', 'raw_dicom')
DPATH_DESCRIPTIONS = Path(tabular.filter_image_descriptions.__file__).parent
FPATH_DESCRIPTIONS = DPATH_DESCRIPTIONS / FNAME_DESCRIPTIONS
FPATH_MANIFEST_RELATIVE = DPATH_TABULAR_RELATIVE / FNAME_MANIFEST
FPATH_STATUS_RELATIVE = DPATH_RAW_DICOM_RELATIVE / FNAME_STATUS
FPATH_LOGS_RELATIVE = Path('scratch', 'logs', 'fetch_dicom_downloads.log')

def run(fpath_global_config, session_id, n_jobs, fname_imaging, datatypes, chunk_size=None, logger=None):

    session_id = session_id_to_bids_session(session_id)

    # parse global config
    with open(fpath_global_config) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])

    # logger
    if logger is None:
        fpath_log = dpath_dataset / FPATH_LOGS_RELATIVE
        logger = my_logger.get_logger(fpath_log)

    logger.info(
        '\n\n===== SETTINGS ====='
        f'\nfpath_global_config: {fpath_global_config}'
        f'\nsession_id: {session_id}'
        f'\nn_jobs: {n_jobs}'
        f'\nfname_imaging: {fname_imaging}'
        f'\ndatatypes: {datatypes}'
        f'\nchunk_size: {chunk_size}'
        f'\ndpath_dataset: {dpath_dataset}'
        '\n'
    )

    # build path to directory containing raw DICOMs for the session
    dpath_raw_dicom_session = dpath_dataset / DPATH_RAW_DICOM_RELATIVE / session_id
    
    # load imaging data
    fpath_imaging = dpath_dataset / DPATH_STUDY_DATA_RELATIVE / fname_imaging
    df_imaging = load_and_process_df_imaging(fpath_imaging)
    df_imaging[COL_SESSION_MANIFEST] = df_imaging[COL_SESSION_MANIFEST].apply(session_id_to_bids_session)

    # load status data
    fpath_status = dpath_dataset / FPATH_STATUS_RELATIVE
    if not fpath_status.exists():
        error_message = f'Status file not found: {fpath_status}. Make sure to run check_dicom_status.py first!'
        logger.error(error_message)
        raise FileNotFoundError(error_message)
    df_status = load_status(fpath_status)
    df_status_session = df_status.loc[df_status[COL_SESSION_MANIFEST] == session_id]

    # load image series descriptions (needed to identify images that are anat/dwi/func)
    if not FPATH_DESCRIPTIONS.exists():
        raise FileNotFoundError(f'Cannot find JSON file containing lists of description strings for datatypes: {FPATH_DESCRIPTIONS}')
    with FPATH_DESCRIPTIONS.open('r') as file_descriptions:
        datatype_descriptions_map: dict = json.load(file_descriptions)

    # gather all relevant series descriptions to download
    descriptions = set()
    for datatype in datatypes:
        descriptions.update(get_all_descriptions(datatype_descriptions_map[datatype]))

    # filter imaging df
    df_imaging_keep = df_imaging.loc[
        (df_imaging[COL_SUBJECT_MANIFEST].isin(df_status_session[COL_SUBJECT_MANIFEST]))
        & (df_imaging[COL_DATATYPE_MANIFEST].isin(descriptions))
    ].copy()
    participants_all = set(df_imaging_keep[COL_SUBJECT_MANIFEST])

    # find participants who have already been downloaded
    participants_downloaded = set(df_status.loc[
        (
            (df_status[COL_SESSION_MANIFEST] == session_id) &
            df_status[COL_DOWNLOAD_STATUS]
        ),
        COL_SUBJECT_MANIFEST,
    ])

    # get image IDs that need to be checked/downloaded
    participants_to_check = participants_all - participants_downloaded
    df_imaging_to_check: pd.DataFrame = df_imaging_keep.loc[
        df_imaging_keep[COL_SUBJECT_MANIFEST].isin(participants_to_check),
    ].copy()

    # check if any image ID has already been downloaded
    check_status = Parallel(n_jobs=n_jobs)(
        delayed(check_image_id)(
            dpath_raw_dicom_session, participant_id, image_id,
        )
        for participant_id, image_id 
        in df_imaging_to_check[[COL_SUBJECT_MANIFEST, COL_IMAGE_ID]].itertuples(index=False)
    )
    df_imaging_to_check[COL_DOWNLOAD_STATUS] = check_status

    # update status file
    participants_to_update = set(df_imaging_to_check.loc[df_imaging_to_check[COL_DOWNLOAD_STATUS], COL_SUBJECT_MANIFEST])
    if len(participants_to_update) > 0:
        df_status_session.loc[df_status_session[COL_SUBJECT_MANIFEST].isin(participants_to_update), COL_DOWNLOAD_STATUS] = True
        df_status.loc[df_status_session.index] = df_status_session
        save_backup(df_status, fpath_status, DNAME_BACKUPS_STATUS)

    logger.info(
        f'\n\n===== {Path(__file__).name.upper()} ====='
        f'\n{len(participants_all)} participant(s) have imaging data for session "{session_id}"'
        f'\n{len(participants_downloaded)} participant(s) already have downloaded data according to the status file'
        f'\n{len(df_imaging_to_check)} images(s) to check ({len(participants_to_check)} participant(s))'
        f'\n\tFound {int(df_imaging_to_check[COL_DOWNLOAD_STATUS].sum())} images already downloaded'
        f'\n\tRemaining {int((~df_imaging_to_check[COL_DOWNLOAD_STATUS]).sum())} images need to be downloaded from LONI'
        f'\nUpdated status for {len(participants_to_update)} participant(s)'
        '\n'
    )

    # get images to download
    image_ids_to_download = df_imaging_to_check.loc[~df_imaging_to_check[COL_DOWNLOAD_STATUS], [COL_SUBJECT_MANIFEST, COL_IMAGE_ID]]
    image_ids_to_download = image_ids_to_download.sort_values(COL_SUBJECT_MANIFEST)

    # output a single chunk if no size is specified
    if chunk_size is None or chunk_size < 1:
        chunk_size = len(image_ids_to_download)
        logger.info(f'Using chunk_size={chunk_size}')

    # dump image ID list into comma-separated list(s)
    n_lists = 0
    download_lists_str = ''
    while len(image_ids_to_download) > 0:
        n_lists += 1

        if download_lists_str != '':
            download_lists_str += '\n\n'

        # generate the list of image IDs to download
        # all images from the same subject must be in the same list
        # so the lists may be smaller than chunk_size
        download_list = []
        for _, image_ids_for_subject in image_ids_to_download.groupby(COL_SUBJECT_MANIFEST):
            if len(download_list) + len(image_ids_for_subject) > chunk_size:
                if len(download_list) == 0:
                    raise RuntimeError(f'chunk_size of {chunk_size} is too small, try increasing to {len(image_ids_for_subject)}')
                break
            download_list.extend(image_ids_for_subject[COL_IMAGE_ID].to_list())

        # build string to print out in one shot by the logger
        download_lists_str += f'LIST {n_lists} ({len(download_list)})\n'
        download_lists_str += ','.join(download_list)

        # update for next iteration
        if len(download_list) < len(image_ids_to_download):
            image_ids_to_download = image_ids_to_download[len(download_list):]
        else:
            image_ids_to_download = []

    logger.info(
        f'\n\n===== DOWNLOAD LIST(S) FOR {session_id.upper()} =====\n'
        f'{download_lists_str}\n'
        '\nCopy the above list(s) into the "Image ID" field in the LONI Advanced Search tool'
        '\nMake sure to check the "DTI", "MRI", and "fMRI" boxes for the "Modality" field'
        '\nCreate a new collection and download the DICOMs, then unzip them in'
        f'\n{dpath_raw_dicom_session} and move the'
        '\nsubject directories outside of the top-level "PPMI" directory'
        '\n'
    )

def check_image_id(dpath_raw_dicom, subject, image_id):
    str_pattern = str(dpath_raw_dicom / subject / '*' / '**' / f'I{image_id}' / '*.dcm')
    return len(list(glob.glob(str_pattern))) > 0

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Find Image IDs for PPMI scans that have not been downloaded yet. Requires an imaging data availability
    info file that can be downloaded from the LONI IDA (should be in <DATASET_ROOT>/{DPATH_STUDY_DATA_RELATIVE}),
    the manifest for PPMI (DATASET_ROOT/{FPATH_MANIFEST_RELATIVE}), as well as the DICOM-to-BIDS conversion status file
    (<DATASET_ROOT>/{FPATH_STATUS_RELATIVE}).
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument('--global_config', type=str, help='path to global config file for your mr_proc dataset', required=True)
    parser.add_argument('--session_id', type=str, default=None, help='MRI session (i.e. visit) to process)', required=True)
    parser.add_argument('--n_jobs', type=int, default=DEFAULT_N_JOBS, help=f'number of parallel processes (default: {DEFAULT_N_JOBS})')
    parser.add_argument('--datatypes', nargs='+', help=f'BIDS datatypes to download (default: {DEFAULT_DATATYPES})', default=DEFAULT_DATATYPES)
    parser.add_argument('--chunk_size', type=int, default=DEFAULT_CHUNK_SIZE, help=f'(default: {DEFAULT_CHUNK_SIZE})')
    parser.add_argument(
        '--imaging_filename', type=str, default=DEFAULT_IMAGING_FILENAME,
        help=('name of file containing imaging data availability info, with columns'
              f' "{COL_SUBJECT_IMAGING}", "{COL_VISIT_IMAGING}", and "{COL_DESCRIPTION_IMAGING}"'
              f'. Expected to be in <DATASET_ROOT>/{DPATH_STUDY_DATA_RELATIVE}'
              f' (default: {DEFAULT_IMAGING_FILENAME})'))

    args = parser.parse_args()
    fpath_global_config = args.global_config
    session_id = args.session_id
    n_jobs = args.n_jobs
    datatypes = args.datatypes
    chunk_size = args.chunk_size
    fname_imaging = args.imaging_filename

    run(fpath_global_config, session_id, n_jobs, fname_imaging, datatypes, chunk_size=chunk_size)
