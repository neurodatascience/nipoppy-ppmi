#!/usr/bin/env python

import argparse

import nibabel
from nipoppy.utils.bids import (
    participant_id_to_bids_participant_id,
    session_id_to_bids_session_id,
)
from nipoppy_ppmi.workflow import BaseDatasetWorkflow


class DwiBvalBvecWorkflow(BaseDatasetWorkflow):
    def __init__(self, participant_id, session_id, **kwargs):
        super().__init__(**kwargs, name="dwi_bval_bvec")
        self.participant_id = participant_id
        self.session_id = session_id

    def run_main(self):

        for (
            participant_id,
            session_id,
        ) in self.curation_status_table.get_bidsified_participants_sessions(
            participant_id=self.participant_id, session_id=self.session_id
        ):

            # find the dwi files
            dpath_dwi = (
                self.layout.dpath_bids
                / participant_id_to_bids_participant_id(participant_id)
                / session_id_to_bids_session_id(session_id)
                / "dwi"
            )

            fpaths_dwi_nii = list(dpath_dwi.glob("*dwi.nii.gz"))
            for fpath_nii in fpaths_dwi_nii:

                self.logger.debug(f"Checking {fpath_nii}")
                fpath_bval = (fpath_nii.parent / fpath_nii.stem).with_suffix(".bval")
                fpath_bvec = (fpath_nii.parent / fpath_nii.stem).with_suffix(".bvec")

                if not (fpath_bval.exists() and fpath_bvec.exists()):

                    if not "acq-B0_" in fpath_nii.name:
                        self.logger.warning(
                            f"Skipping {fpath_nii} since filename does not imply that it is a B0 image, should check"
                        )
                        continue

                    # load the file and check the number of volumes
                    img = nibabel.load(fpath_nii)
                    if len(img.shape) == 4:
                        n_volumnes = img.shape[-1]
                    elif len(img.shape) == 3:
                        n_volumnes = 1
                    else:
                        self.logger.error(
                            f"Image {fpath_nii} has {len(img.shape)} dimensions, not sure how to handle"
                        )
                        continue
                    self.logger.debug(f"Image {fpath_nii} has {n_volumnes} volumes")

                    self.logger.info(f"Adding bval and bvec files for {fpath_nii}")

                    # write bval/bvec files
                    if not self.dry_run:
                        bvals = " ".join(["0"] * n_volumnes)
                        bvecs = "\n".join([bvals] * 3)
                        fpath_bval.write_text(f"{bvals}\n")
                        fpath_bvec.write_text(f"{bvecs}\n")

                elif (not fpath_bval.exists()) or (not fpath_bvec.exists()):
                    self.logger.error(
                        f"Only one of {fpath_bval} and {fpath_bvec} exists, not sure how to handle"
                    )
                    continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Make sure that all dwi files have a corresponding bval and bvec file."
        ),
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--participant-id", required=False)
    parser.add_argument("--session-id", required=False)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    workflow = DwiBvalBvecWorkflow(
        participant_id=args.participant_id,
        session_id=args.session_id,
        dpath_root=args.dataset,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    try:
        workflow.run()
    except Exception:
        workflow.logger.exception("An error occurred while running the workflow.")
