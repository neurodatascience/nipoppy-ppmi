#!/usr/bin/env python
"""DICOM file organization for PPMI (following old)."""

import argparse
import logging
import os
import re
from functools import cached_property
from pathlib import Path
from rich_argparse import RichHelpFormatter

import pandas as pd
import pydicom

from nipoppy.logger import add_logfile
from nipoppy.utils import (
    participant_id_to_bids_participant_id,
    session_id_to_bids_session_id,
)
from nipoppy.workflows import DicomReorgWorkflow

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.env import COL_IMAGE_ID
from nipoppy_ppmi.imaging_utils import RE_IMAGE_ID
from nipoppy_ppmi.tabular_utils import load_and_process_df_imaging


def is_derived_dicom(fpath: Path) -> bool:
    """
    Read a DICOM file's header and check if it is a derived file.

    Some BIDS converters (e.g. Heudiconv) do not support derived DICOM files.
    """
    dcm_info = pydicom.dcmread(fpath)
    img_types = dcm_info.ImageType
    return "DERIVED" in img_types


class CustomDicomReorgWorkflow(DicomReorgWorkflow):
    """Workflow for organizing raw DICOM files."""

    @cached_property
    def custom_config(self) -> CustomConfig:
        return CustomConfig(**self.config.CUSTOM)

    @cached_property
    def df_imaging_info(self) -> pd.DataFrame:
        return load_and_process_df_imaging(self.custom_config.IMAGING_INFO.FILEPATH)

    @cached_property
    def image_ids(self) -> set:
        return set(self.df_imaging_info[COL_IMAGE_ID])

    def apply_fname_mapping(
        self,
        fpath_source: Path,
        participant_id: str,
        session_id: str,
    ) -> str:
        # get image ID from directory name
        if match := re.search(RE_IMAGE_ID, str(fpath_source)):
            if len(match.groups()) > 1:
                raise RuntimeError(
                    f"More than one possible image ID for {fpath_source}"
                    f": {match.groups()}"
                )
            image_id = match.group(1)
        else:
            raise RuntimeError(f"No image ID found in {fpath_source}")

        # make sure image ID (without 'I') is in imaging info
        image_id_short = image_id[1:]
        if image_id_short not in self.image_ids:
            raise RuntimeError(
                f"Image ID {image_id_short} not found in imaging info file"
            )

        # if image ID is not in filename, add it to filename
        if image_id not in fpath_source.name:
            return f"{fpath_source.stem}_{image_id}{fpath_source.suffix}"
        else:
            return fpath_source.name

    def run_single(self, participant_id: str, session_id: str):
        """Reorganize downloaded DICOM files for a single participant and session."""
        # get paths to reorganize
        fpaths_to_reorg = self.get_fpaths_to_reorg(participant_id, session_id)

        dpath_reorganized: Path = (
            self.layout.dpath_post_reorg
            / participant_id_to_bids_participant_id(participant_id)
            / session_id_to_bids_session_id(session_id)
        )
        self.mkdir(dpath_reorganized)

        # do reorg
        for fpath_source in fpaths_to_reorg:
            # check file (though only error out if DICOM cannot be read)
            if self.check_dicoms:
                try:
                    if is_derived_dicom(fpath_source):
                        self.logger.warning(
                            f"Derived DICOM file detected: {fpath_source}"
                        )
                except Exception as exception:
                    raise RuntimeError(
                        f"Error checking DICOM file {fpath_source}: {exception}"
                    )

            # DIFFERENT FROM DEFAULT SCRIPT
            # passing entire fpath instead of just fname
            fpath_dest = (
                dpath_reorganized
                / self.apply_fname_mapping(
                    fpath_source, participant_id=participant_id, session_id=session_id
                )
            ).resolve()

            # do not overwrite existing files
            if fpath_dest.exists():
                raise FileExistsError(
                    f"Cannot move file {fpath_source} to {fpath_dest}"
                    " because it already exists"
                )

            # either create symlinks or copy original files
            if self.copy_files:
                self.copy(fpath_source, fpath_dest, log_level=logging.DEBUG)
            else:
                fpath_source = os.path.relpath(
                    fpath_source.resolve(), fpath_dest.parent
                )
                self.create_symlink(
                    path_source=fpath_source,
                    path_dest=fpath_dest,
                    log_level=logging.DEBUG,
                )

        # update doughnut entry
        self.doughnut.set_status(
            participant_id=participant_id,
            session_id=session_id,
            col=self.doughnut.col_in_post_reorg,
            status=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the custom DICOM reorganization workflow.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        required=True,
        help="Root directory of Nipoppy dataset",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually copy/symlink files",
    )
    args = parser.parse_args()

    # initialize workflow
    workflow = CustomDicomReorgWorkflow(
        dpath_root=args.dataset_root,
        dry_run=args.dry_run,
    )

    # set up logging to a file
    logger = workflow.logger
    # logger.setLevel(logging.DEBUG)
    add_logfile(logger, workflow.generate_fpath_log())

    # run the workflow
    try:
        workflow.run()
    except Exception:
        logger.exception(
            "An error occurred with the custom DICOM reorganization script"
        )
