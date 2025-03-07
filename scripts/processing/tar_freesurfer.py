#!/usr/bin/env python

import argparse
import sys
from pathlib import Path
from tarfile import is_tarfile

from nipoppy.workflows import BaseWorkflow
from nipoppy.env import EXT_TAR

FS_FMRIPREP_MAP = {
    "7.3.2": "23.1.3",
    "6.0.1": "20.2.7",
}


class TarFreesurferWorkflow(BaseWorkflow):

    def __init__(self, dpath_root, fs_version, **kwargs):
        self.fs_version = fs_version
        try:
            self.fmriprep_version = FS_FMRIPREP_MAP[fs_version]
        except KeyError:
            raise ValueError(f"Invalid FreeSurfer version: {self.fs_version}")
        super().__init__(
            dpath_root=dpath_root,
            name="tar_freesurfer",
            # _skip_logging=True,
            **kwargs,
        )

    def run_main(self):
        dpath_fmriprep = self.layout.get_dpath_pipeline_output(
            "fmriprep", self.fmriprep_version
        )
        if not dpath_fmriprep.exists():
            raise ValueError(f"fMRIPrep directory not found: {dpath_fmriprep}")
        dpath_freesurfer = self.layout.get_dpath_pipeline_output(
            "freesurfer", self.fs_version
        )
        if not dpath_freesurfer.exists():
            raise ValueError(f"FreeSurfer directory not found: {dpath_freesurfer}")

        for dpath_session in dpath_freesurfer.glob("ses-*"):
            bids_session_id = dpath_session.name
            self.logger.info(f"Checking session: {bids_session_id}")

            for path_participant in dpath_session.glob("sub-*"):
                if path_participant.suffix == ".tar":
                    continue

                bids_participant_id = path_participant.name

                # check if fMRIPrep tarball exists
                if self.fmriprep_version == "23.1.3":
                    fpath_fmriprep_tarball = (
                        dpath_fmriprep / bids_participant_id / f"{bids_session_id}.tar"
                    )
                elif self.fmriprep_version == "20.2.7":
                    fpath_fmriprep_tarball = (
                        dpath_fmriprep
                        / "fmriprep"
                        / bids_participant_id
                        / f"{bids_session_id}.tar"
                    )

                if fpath_fmriprep_tarball.exists():
                    self.tar_directory(path_participant)

    def tar_directory(self, dpath: Path) -> Path:
        """Tar a directory and delete it."""
        if not dpath.exists():
            raise RuntimeError(f"Not tarring {dpath} since it does not exist")
        if not dpath.is_dir():
            raise RuntimeError(f"Not tarring {dpath} since it is not a directory")

        tar_flags = "-cvf"
        fpath_tarred = dpath.with_suffix(EXT_TAR)

        self.run_command(
            f"tar {tar_flags} {fpath_tarred} -C {dpath.parent} {dpath.name}"
        )

        # make sure that the tarfile was created successfully before removing
        # original directory
        if fpath_tarred.exists() and is_tarfile(fpath_tarred):
            self.rm(dpath)
        else:
            self.logger.error(f"Failed to tar {dpath} to {fpath_tarred}")

        return fpath_tarred


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tar FreeSurfer output")
    parser.add_argument("dpath_root", type=str, help="Path to the root directory")
    parser.add_argument(
        "--fs-version",
        type=str,
        required=False,
        default="7.3.2",
        help="FreeSurfer version",
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    workflow = TarFreesurferWorkflow(
        args.dpath_root, args.fs_version, dry_run=args.dry_run
    )
    workflow.run()
