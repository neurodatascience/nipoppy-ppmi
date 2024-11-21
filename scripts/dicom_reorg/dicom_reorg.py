#!/usr/bin/env python
"""DICOM file organization for PPMI (following old)."""

import argparse
import logging
import os
from pathlib import Path
from rich_argparse import RichHelpFormatter

import pydicom

from nipoppy.logger import add_logfile
from nipoppy.utils import session_id_to_bids_session_id
from nipoppy.workflows import DicomReorgWorkflow


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

    def apply_fname_mapping(
        self,
        fpath_source: str,
        participant_id: str,
        session_id: str,
    ) -> str:
        # TODO get image ID from directory name
        # if image ID is not in filename, add it to filename
        return super().apply_fname_mapping(
            fpath_source.name, participant_id, session_id
        )

    def run_single(self, participant_id: str, session_id: str):
        """Reorganize downloaded DICOM files for a single participant and session."""
        # get paths to reorganize
        fpaths_to_reorg = self.get_fpaths_to_reorg(participant_id, session_id)

        # DIFFERENT FROM DEFAULT SCRIPT
        dpath_reorganized: Path = (
            self.layout.dpath_sourcedata
            / session_id_to_bids_session_id(session_id)
            / participant_id
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

            fpath_dest = dpath_reorganized / self.apply_fname_mapping(
                fpath_source, participant_id=participant_id, session_id=session_id
            )

            # do not overwrite existing files
            if fpath_dest.exists():
                raise FileExistsError(
                    f"Cannot move file {fpath_source} to {fpath_dest}"
                    " because it already exists"
                )

            # either create symlinks or copy original files
            if not self.dry_run:
                if self.copy_files:
                    self.copy(fpath_source, fpath_dest, log_level=logging.DEBUG)
                else:
                    fpath_source = os.path.relpath(fpath_source, fpath_dest.parent)
                    self.create_symlink(
                        path_source=fpath_source,
                        path_dest=fpath_dest,
                        log_level=logging.DEBUG,
                    )

        # update doughnut entry
        self.doughnut.set_status(
            participant_id=participant_id,
            session_id=session_id,
            col=self.doughnut.col_in_sourcedata,
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
    args = parser.parse_args()

    # initialize workflow
    workflow = CustomDicomReorgWorkflow(dpath_root=args.dataset_root)

    # set up logging to a file
    logger = workflow.logger
    add_logfile(logger, workflow.generate_fpath_log())

    # run the workflow
    try:
        workflow.run()
    except Exception:
        logger.exception(
            "An error occurred with the custom DICOM reorganization script"
        )
