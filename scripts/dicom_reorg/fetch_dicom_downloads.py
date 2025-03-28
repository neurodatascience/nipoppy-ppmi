#!/usr/bin/env python

import argparse
import json
import logging
from pathlib import Path

import pandas as pd
from joblib import Parallel, delayed
from nipoppy.logger import add_logfile
from nipoppy.tabular import Doughnut, Manifest
from nipoppy.workflows import BaseWorkflow
from rich_argparse import RichHelpFormatter

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.env import (
    COL_IMAGE_ID,
    DATATYPE_ANAT,
    DATATYPE_DWI,
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


def _check_image_id(dpath_raw_dicom, image_id):
    return next(Path(dpath_raw_dicom).glob(f"**/I{image_id}/*.dcm"), None) is not None


class FetchDicomDownloadsWorkflow(BaseWorkflow):

    def __init__(
        self,
        session_id,
        n_jobs,
        datatypes,
        chunk_size=None,
        **kwargs,
    ):
        super().__init__(name=Path(__file__).stem, **kwargs)
        self.session_id = session_id
        self.n_jobs = n_jobs
        self.datatypes = datatypes
        self.chunk_size = chunk_size

    def run_main(self):

        # get custom config
        custom_config = CustomConfig(**self.config.CUSTOM)

        # load imaging data
        fpath_imaging = custom_config.IMAGING_INFO.FILEPATH
        if not fpath_imaging.exists():
            raise FileNotFoundError(
                f"Cannot find imaging data information file: {fpath_imaging}"
            )
        df_imaging = load_and_process_df_imaging(fpath_imaging)
        self.logger.debug(f"\nLoaded imaging data availability file\n{df_imaging}")

        # load image series descriptions (needed to identify images that are anat/dwi/func)
        self.fpath_descriptions = custom_config.IMAGE_DESCRIPTIONS.FILEPATH
        if not self.fpath_descriptions.exists():
            raise FileNotFoundError(
                f"Cannot find JSON file containing lists of description strings for datatypes: {self.fpath_descriptions}"
            )
        with self.fpath_descriptions.open("r") as file_descriptions:
            datatype_descriptions_map: dict = json.load(file_descriptions)

        # gather all relevant series descriptions to download
        descriptions = set()
        for datatype in self.datatypes:
            descriptions.update(
                get_all_descriptions(datatype_descriptions_map[datatype])
            )

        # filter imaging df
        df_imaging_keep = df_imaging.loc[
            (
                df_imaging[Manifest.col_participant_id].isin(
                    self.doughnut.get_imaging_subset(session_id=self.session_id)[
                        Manifest.col_participant_id
                    ]
                )
            )
            & (df_imaging[Manifest.col_session_id] == self.session_id)
            & (df_imaging[Manifest.col_datatype].isin(descriptions))
        ].copy()
        participants_all = set(
            [
                p
                for p, _ in self.manifest.get_participants_sessions(
                    session_id=self.session_id
                )
            ]
        )
        self.logger.info(
            f'{len(participants_all)} participant(s) have imaging data for session "{self.session_id}"'
        )

        # find participants that have already been downloaded/reorganized/bidsified
        participants_downloaded = set(
            [
                p
                for p, _ in self.doughnut.get_downloaded_participants_sessions(
                    session_id=self.session_id
                )
            ]
        )
        participants_downloaded.update(
            [
                p
                for p, _ in self.doughnut.get_organized_participants_sessions(
                    session_id=self.session_id
                )
            ]
        )
        participants_downloaded.update(
            [
                p
                for p, _ in self.doughnut.get_bidsified_participants_sessions(
                    session_id=self.session_id
                )
            ]
        )
        self.logger.info(
            f"{len(participants_downloaded)} participant(s) already have data on disk according to the status file"
        )

        # get image IDs that need to be checked/downloaded
        participants_to_check = participants_all - participants_downloaded
        df_imaging_to_check: pd.DataFrame = df_imaging_keep.loc[
            df_imaging_keep[Manifest.col_participant_id].isin(participants_to_check),
        ].copy()

        self.logger.info(
            f"{len(df_imaging_to_check)} images(s) to check ({len(participants_to_check)} participant(s))"
        )

        # check if any image ID has already been downloaded
        check_status = Parallel(n_jobs=self.n_jobs)(
            delayed(_check_image_id)(
                self.layout.dpath_pre_reorg
                / self.dicom_dir_map.get_dicom_dir(
                    participant_id=participant_id, session_id=self.session_id
                ),
                image_id,
            )
            for participant_id, image_id in df_imaging_to_check[
                [Manifest.col_participant_id, COL_IMAGE_ID]
            ].itertuples(index=False)
        )
        df_imaging_to_check[Doughnut.col_in_pre_reorg] = check_status

        self.logger.info(
            f"\tFound {int(df_imaging_to_check[Doughnut.col_in_pre_reorg].sum())} images already downloaded"
        )
        self.logger.info(
            f"\tRemaining {int((~df_imaging_to_check[Doughnut.col_in_pre_reorg]).sum())} images need to be downloaded from LONI"
        )

        # update status file
        participants_to_update = set(
            df_imaging_to_check.loc[
                df_imaging_to_check[Doughnut.col_in_pre_reorg],
                Manifest.col_participant_id,
            ]
        )
        for participant_id in participants_to_update:
            self.doughnut.set_status(
                participant_id=participant_id,
                session_id=self.session_id,
                col=Doughnut.col_in_pre_reorg,
                status=True,
            )
        self.logger.info(
            f"Updated status for {len(participants_to_update)} participant(s)"
        )
        self.save_tabular_file(self.doughnut, self.layout.fpath_doughnut)

        # get images to download
        image_ids_to_download = df_imaging_to_check.loc[
            ~df_imaging_to_check[Doughnut.col_in_pre_reorg],
            [Manifest.col_participant_id, COL_IMAGE_ID],
        ]
        image_ids_to_download = image_ids_to_download.sort_values(
            Manifest.col_participant_id
        )

        # output a single chunk if no size is specified
        if self.chunk_size is None or self.chunk_size < 1:
            self.chunk_size = len(image_ids_to_download)

        # dump image ID list into comma-separated list(s)
        self.logger.info(f"----- DOWNLOAD LIST(S) FOR {self.session_id.upper()} -----")
        n_lists = 0
        while len(image_ids_to_download) > 0:
            n_lists += 1

            # if download_lists_str != "":
            #     download_lists_str += "\n\n"

            # generate the list of image IDs to download
            # all images from the same subject must be in the same list
            # so the lists may be smaller than chunk_size
            download_list = []
            for _, image_ids_for_subject in image_ids_to_download.groupby(
                Manifest.col_participant_id
            ):
                if len(download_list) + len(image_ids_for_subject) > self.chunk_size:
                    if len(download_list) == 0:
                        raise RuntimeError(
                            f"chunk_size of {self.chunk_size} is too small, try increasing to {len(image_ids_for_subject)}"
                        )
                    break
                download_list.extend(image_ids_for_subject[COL_IMAGE_ID].to_list())

            # build string to print out in one shot by the logger
            self.logger.info(
                f"LIST {n_lists} ({len(download_list)})\n{','.join(download_list)}"
            )

            # update for next iteration
            if len(download_list) < len(image_ids_to_download):
                image_ids_to_download = image_ids_to_download[len(download_list) :]
            else:
                image_ids_to_download = []

        self.logger.info(
            'Copy the above list(s) into the "Image ID" field in the LONI Advanced Search tool'
        )
        self.logger.info(
            'Make sure to check the "DTI", "MRI", and "fMRI" boxes for the "Modality" field'
        )
        self.logger.info(
            "Create a new collection and download the DICOMs, then unzip them in"
        )
        self.logger.info("the raw DICOM directory and move the")
        self.logger.info(
            'subject directories outside of the top-level "PPMI" directory'
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find image IDs for PPMI scans that have not been downloaded yet.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        help="Path to dataset root",
        required=True,
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
        help=f"BIDS datatypes to consider (default: {DEFAULT_DATATYPES})",
        default=DEFAULT_DATATYPES,
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Number of image IDs to be printed in a chunk (default: {DEFAULT_CHUNK_SIZE})",
    )

    args = parser.parse_args()

    workflow = FetchDicomDownloadsWorkflow(
        dpath_root=args.dataset.expanduser(),
        session_id=args.session_id,
        n_jobs=args.n_jobs,
        datatypes=args.datatypes,
        chunk_size=args.chunk_size,
    )
    workflow.logger.setLevel(logging.DEBUG)

    try:
        workflow.run()
    except Exception as e:
        workflow.logger.exception(
            "An error occurred while generating the image ID lists"
        )
