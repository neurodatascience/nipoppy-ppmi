#!/usr/bin/env python

import argparse
import hashlib
import logging
import shutil
import sys
from pathlib import Path

import pandas as pd
from bids.layout import parse_file_entities
from nipoppy.workflows import BaseWorkflow


class CleanUpBidsWorkflow(BaseWorkflow):

    def __init__(self, dpath_root, **kwargs):
        super().__init__(
            dpath_root=dpath_root,
            name="clean_up_bids",
            # _skip_logging=True,
            **kwargs,
        )

    def delete_heudiconv_files(self):
        """Find and delete all HeuDiConv temporary files."""

        # only check subject directories
        # because we don't want to delete the .heudiconv-*.tar.gz files
        count = 0
        for path_heudiconv in self.layout.dpath_bids.glob("sub-*/**/*heudiconv*"):
            if path_heudiconv.is_file():
                self.rm(path_heudiconv, log_level=logging.DEBUG)
                count += 1
        self.logger.info(f"Deleted {count} HeuDiConv files")

    def check_md5_same(self, fpaths):
        past_md5 = None
        for fpath in fpaths:
            new_md5 = hashlib.md5(Path(fpath).read_bytes()).hexdigest()
            if past_md5 is not None and new_md5 != past_md5:
                return False
            else:
                past_md5 = new_md5
        return True

    def move_file(self, fpath_src, fpath_dest, log_level=logging.INFO, **kwargs):
        """Move a file."""
        self.logger.log(level=log_level, msg=f"Moving {fpath_src} -> {fpath_dest}")
        if not self.dry_run:
            shutil.move(fpath_src, fpath_dest, **kwargs)

    def fix_duplicate_scans(self):

        count = 0
        total = 0
        for fpath in self.layout.dpath_bids.rglob("*1.nii.gz"):
            if "heudiconv" in str(fpath):
                continue

            total += 1

            prefix = fpath.name.removesuffix("1.nii.gz")
            self.logger.info(f"===== {fpath.parent / prefix}* =====")

            # find matching scans
            fpaths_duplicated_nii = set(fpath.parent.glob(f"{prefix}*.nii.gz"))
            fpaths_duplicated_json = set(fpath.parent.glob(f"{prefix}*.json"))

            self.logger.debug(f"{len(fpaths_duplicated_nii)=}")
            self.logger.debug(f"{len(fpaths_duplicated_json)=}")

            # find scans.tsv file
            dpath_session = fpath.parent.parent
            entities = parse_file_entities(fpath)
            fpath_scans = (
                dpath_session
                / f'sub-{entities["subject"]}_ses-{entities["session"]}_scans.tsv'
            )
            if not fpath_scans.exists():
                self.logger.warning(f"{fpath_scans} does not exist")
            else:
                self.logger.debug(f"{fpath_scans=}")

            # confirm that they are identical
            if not (
                self.check_md5_same(fpaths_duplicated_nii)
                and self.check_md5_same(fpaths_duplicated_json)
            ):
                self.logger.warning(f"Checksums are not identical, skipping")
                continue

            count += 1

            # rename one of them
            fpath_to_move_nii = fpaths_duplicated_nii.pop()
            fpath_to_move_json = fpaths_duplicated_json.pop()
            self.move_file(
                fpath_to_move_nii, fpath_to_move_nii.parent / f"{prefix}.nii.gz"
            )
            self.move_file(
                fpath_to_move_json, fpath_to_move_json.parent / f"{prefix}.json"
            )
            self.logger.debug(f"{len(fpaths_duplicated_nii)=}")
            self.logger.debug(f"{len(fpaths_duplicated_json)=}")

            # delete the rest
            count_deleted = 0
            for fpath in fpaths_duplicated_nii | fpaths_duplicated_json:
                self.rm(fpath, log_level=logging.DEBUG)
                count_deleted += 1
            self.logger.info(f"Deleted {count_deleted} duplicate nii.gz/json files")

            # fix scans.tsv file
            if fpath_scans.exists():
                df_scans = pd.read_csv(
                    fpath_scans, sep="\t", dtype=str, keep_default_na=False
                )
                n_rows_before = df_scans.shape[0]

                # delete
                filenames_to_remove = [
                    str(fpath.relative_to(dpath_session))
                    for fpath in fpaths_duplicated_nii
                ]
                df_scans = df_scans.loc[
                    ~(df_scans["filename"].isin(filenames_to_remove))
                ]
                n_rows_after = df_scans.shape[0]
                self.logger.info(
                    f"Removing {n_rows_before - n_rows_after} rows from {fpath_scans}"
                )

                # rename
                df_scans.loc[
                    df_scans["filename"]
                    == str(fpath_to_move_nii.relative_to(dpath_session)),
                    "filename",
                ] = str(
                    (fpath_to_move_nii.parent / f"{prefix}.nii.gz").relative_to(
                        dpath_session
                    )
                )

                if not self.dry_run:
                    df_scans.to_csv(fpath_scans, sep="\t", index=False)

        self.logger.info(f"Fixed {count} (out of {total}) instances of duplicate scans")

    def run_main(self):

        self.logger.info(f"Deleting HeuDiConv files")
        self.delete_heudiconv_files()

        self.logger.info(f"Fixing duplicate scans")
        self.fix_duplicate_scans()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean up BIDS directory after conversion (remove heudiconv files and duplicate scans)"
    )
    parser.add_argument("dpath_root", type=str, help="Path to the root directory")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    workflow = CleanUpBidsWorkflow(
        args.dpath_root,
        dry_run=args.dry_run,
        # verbose=True,
    )
    workflow.run()
