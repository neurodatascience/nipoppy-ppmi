#!/usr/bin/env python

import argparse
import glob
import json
import logging
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed
from nipoppy.cli.parser import add_arg_dataset_root
from nipoppy.layout import DEFAULT_LAYOUT_INFO
from nipoppy.logger import add_logfile
from nipoppy.utils import session_id_to_bids_session
from nipoppy.tabular import Doughnut, Manifest
from nipoppy.workflows import BaseWorkflow
from rich_argparse import RichHelpFormatter

from nipoppy_ppmi.env import (
    COL_IMAGE_ID,
    DEFAULT_FNAME_IMAGING_DESCRIPTIONS,
    DATATYPE_ANAT,
    DATATYPE_DWI,
    DATATYPE_FUNC,
)
from nipoppy_ppmi.imaging_utils import get_all_descriptions
from nipoppy_ppmi.tabular_utils import load_and_process_df_imaging

# default command-line arguments
DEFAULT_DATATYPES = [DATATYPE_ANAT, DATATYPE_DWI]
DEFAULT_N_JOBS = 4
DEFAULT_CHUNK_SIZE = 1000

# DPATH_TABULAR_RELATIVE = Path("tabular")
# DPATH_RAW_DICOM_RELATIVE = Path("scratch", "raw_dicom")
# DPATH_DESCRIPTIONS = Path(
#     nipoppy.workflow.tabular.filter_image_descriptions.__file__
# ).parent
# FPATH_DESCRIPTIONS = DPATH_DESCRIPTIONS / FNAME_DESCRIPTIONS
# FPATH_MANIFEST_RELATIVE = DPATH_TABULAR_RELATIVE / FNAME_MANIFEST
# FPATH_STATUS_RELATIVE = DPATH_RAW_DICOM_RELATIVE / FNAME_DOUGHNUT
# FPATH_LOGS_RELATIVE = Path("scratch", "logs", "fetch_dicom_downloads.log")


def _check_image_id(dpath_raw_dicom, subject, image_id):
    str_pattern = str(dpath_raw_dicom / subject / "*" / "**" / f"I{image_id}" / "*.dcm")
    return len(list(glob.glob(str_pattern))) > 0


class FetchDicomDownloadsWorkflow(BaseWorkflow):

    def __init__(
        self,
        fpath_imaging,
        fpath_descriptions,
        session_id,
        n_jobs,
        datatypes,
        chunk_size=None,
        **kwargs,
    ):
        super().__init__(name="fetch_dicom_downloads", **kwargs)
        self.fpath_imaging = Path(fpath_imaging)
        self.fpath_descriptions = Path(fpath_descriptions)
        self.session_id = session_id_to_bids_session(session_id)
        self.n_jobs = n_jobs
        self.datatypes = datatypes
        self.chunk_size = chunk_size

    def run_main(self):

        # build path to directory containing raw DICOMs for the session
        dpath_raw_dicom_session = self.layout.dpath_raw_imaging / self.session_id

        # load imaging data
        if not self.fpath_imaging.exists():
            raise FileNotFoundError(
                f"Cannot find imaging data information file: {self.fpath_imaging}"
            )
        df_imaging = load_and_process_df_imaging(self.fpath_imaging)
        df_imaging[Manifest.col_session_id] = df_imaging[Manifest.col_session_id].apply(
            session_id_to_bids_session
        )

        # load status data
        df_doughnut_session: Doughnut = self.doughnut.get_imaging_subset(
            session_id=self.session_id
        )

        # load image series descriptions (needed to identify images that are anat/dwi/func)
        if not self.fpath_descriptions.exists():
            raise FileNotFoundError(
                f"Cannot find JSON file containing lists of description strings for datatypes: {self.fpath_descriptions}"
            )
        with self.fpath_descriptions.open("r") as file_descriptions:
            datatype_descriptions_map: dict = json.load(file_descriptions)

        # gather all relevant series descriptions to download
        descriptions = set()
        for datatype in datatypes:
            descriptions.update(
                get_all_descriptions(datatype_descriptions_map[datatype])
            )

        # filter imaging df
        df_imaging_keep = df_imaging.loc[
            (
                df_imaging[Manifest.col_participant_id].isin(
                    df_doughnut_session[Manifest.col_participant_id]
                )
            )
            & (df_imaging[Manifest.col_session_id] == self.session_id)
            & (df_imaging[Manifest.col_datatype].isin(descriptions))
        ].copy()
        participants_all = set(df_imaging_keep[Manifest.col_participant_id])

        # find participants who have already been downloaded
        participants_downloaded = set(
            df_doughnut_session.get_downloaded_participants_sessions()[
                Manifest.col_participant_id
            ]
            # df_status.loc[
            #     (
            #         (df_status[Manifest.col_session_id] == session_id)
            #         & df_status[Doughnut.col_in_raw_imaging]
            #     ),
            #     Manifest.col_participant_id,
            # ]
        )

        # get image IDs that need to be checked/downloaded
        participants_to_check = participants_all - participants_downloaded
        df_imaging_to_check: pd.DataFrame = df_imaging_keep.loc[
            df_imaging_keep[Manifest.col_participant_id].isin(participants_to_check),
        ].copy()

        # check if any image ID has already been downloaded
        check_status = Parallel(n_jobs=n_jobs)(
            delayed(_check_image_id)(
                dpath_raw_dicom_session,
                participant_id,
                image_id,
            )
            for participant_id, image_id in df_imaging_to_check[
                [Manifest.col_participant_id, COL_IMAGE_ID]
            ].itertuples(index=False)
        )
        df_imaging_to_check[Doughnut.col_in_raw_imaging] = check_status

        # update status file
        participants_to_update = set(
            df_imaging_to_check.loc[
                df_imaging_to_check[Doughnut.col_in_raw_imaging],
                Manifest.col_participant_id,
            ]
        )
        for participant_id in participants_to_update:
            self.doughnut.set_status(
                participant_id=participant_id,
                session_id=session_id,
                col=Doughnut.col_in_raw_imaging,
                status=True,
            )
            # df_doughnut_session.loc[
            #     df_doughnut_session[Manifest.col_participant_id].isin(
            #         participants_to_update
            #     ),
            #     Doughnut.col_in_raw_imaging,
            # ] = True
            # self.doughnut.loc[df_doughnut_session.index] = df_doughnut_session
        self.save_tabular_file(self.doughnut, self.layout.fpath_doughnut)

        self.logger.info(
            f"\n\n===== {Path(__file__).name.upper()} ====="
            f'\n{len(participants_all)} participant(s) have imaging data for session "{session_id}"'
            f"\n{len(participants_downloaded)} participant(s) already have downloaded data according to the status file"
            f"\n{len(df_imaging_to_check)} images(s) to check ({len(participants_to_check)} participant(s))"
            f"\n\tFound {int(df_imaging_to_check[Doughnut.col_in_raw_imaging].sum())} images already downloaded"
            f"\n\tRemaining {int((~df_imaging_to_check[Doughnut.col_in_raw_imaging]).sum())} images need to be downloaded from LONI"
            f"\nUpdated status for {len(participants_to_update)} participant(s)"
            "\n"
        )

        # get images to download
        image_ids_to_download = df_imaging_to_check.loc[
            ~df_imaging_to_check[Doughnut.col_in_raw_imaging],
            [Manifest.col_participant_id, COL_IMAGE_ID],
        ]
        image_ids_to_download = image_ids_to_download.sort_values(
            Manifest.col_participant_id
        )

        # output a single chunk if no size is specified
        if chunk_size is None or chunk_size < 1:
            chunk_size = len(image_ids_to_download)
            self.logger.info(f"Using chunk_size={chunk_size}")

        # dump image ID list into comma-separated list(s)
        n_lists = 0
        download_lists_str = ""
        while len(image_ids_to_download) > 0:
            n_lists += 1

            if download_lists_str != "":
                download_lists_str += "\n\n"

            # generate the list of image IDs to download
            # all images from the same subject must be in the same list
            # so the lists may be smaller than chunk_size
            download_list = []
            for _, image_ids_for_subject in image_ids_to_download.groupby(
                Manifest.col_participant_id
            ):
                if len(download_list) + len(image_ids_for_subject) > chunk_size:
                    if len(download_list) == 0:
                        raise RuntimeError(
                            f"chunk_size of {chunk_size} is too small, try increasing to {len(image_ids_for_subject)}"
                        )
                    break
                download_list.extend(image_ids_for_subject[COL_IMAGE_ID].to_list())

            # build string to print out in one shot by the logger
            download_lists_str += f"LIST {n_lists} ({len(download_list)})\n"
            download_lists_str += ",".join(download_list)

            # update for next iteration
            if len(download_list) < len(image_ids_to_download):
                image_ids_to_download = image_ids_to_download[len(download_list) :]
            else:
                image_ids_to_download = []

        self.logger.info(
            f"\n\n===== DOWNLOAD LIST(S) FOR {session_id.upper()} =====\n"
            f"{download_lists_str}\n"
            '\nCopy the above list(s) into the "Image ID" field in the LONI Advanced Search tool'
            '\nMake sure to check the "DTI", "MRI", and "fMRI" boxes for the "Modality" field'
            "\nCreate a new collection and download the DICOMs, then unzip them in"
            f"\n{dpath_raw_dicom_session} and move the"
            '\nsubject directories outside of the top-level "PPMI" directory'
            "\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find image IDs for PPMI scans that have not been downloaded yet.",
        formatter_class=RichHelpFormatter,
    )
    add_arg_dataset_root(parser)
    parser.add_argument(
        "--fpath-imaging",
        type=Path,
        required=True,
        help="Path to the imaging data CSV file, relative to the dataset root",
    )
    parser.add_argument(
        "--fpath-imaging-descriptions",
        type=Path,
        required=True,
        help="Absolute path to the JSON file containing image descriptions for each datatype",
    )
    parser.add_argument(
        "--session-id",
        type=str,
        default=None,
        help="Session ID to process (without BIDS prefix)",
        required=True,
    )
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=DEFAULT_N_JOBS,
        help=f"number of parallel processes (default: {DEFAULT_N_JOBS})",
    )
    parser.add_argument(
        "--datatypes",
        nargs="+",
        help=f"BIDS datatypes to download (default: {DEFAULT_DATATYPES})",
        default=DEFAULT_DATATYPES,
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Number of image IDs to be printed in a chunk (default: {DEFAULT_CHUNK_SIZE})",
    )

    args = parser.parse_args()
    # fpath_global_config = args.global_config
    # session_id = args.session_id
    # n_jobs = args.n_jobs
    # datatypes = args.datatypes
    # chunk_size = args.chunk_size

    workflow = FetchDicomDownloadsWorkflow(
        dpath_root=args.dataset_root,
        fpath_imaging=args.fpath_imaging,
        fpath_descriptions=args.fpath_imaging_descriptions,
        session_id=args.session_id,
        n_jobs=args.n_jobs,
        datatypes=args.datatypes,
        chunk_size=args.chunk_size,
    )
    workflow.logger.setLevel(logging.DEBUG)
    add_logfile(workflow.logger, workflow.generate_fpath_log())

    try:
        workflow.run()
    except Exception as e:
        workflow.logger.exception(
            "An error occurred while generating the image ID lists"
        )
