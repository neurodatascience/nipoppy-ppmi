#!/usr/bin/env python

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from nipoppy.cli.parser import (
    add_arg_dataset_root,
    add_arg_verbosity,
    VERBOSITY_TO_LOG_LEVEL_MAP,
)
from nipoppy.utils import (
    check_participant_id,
    check_session_id,
)
from nipoppy.workflows import BaseWorkflow
from nipoppy.logger import add_logfile, capture_warnings
from rich_argparse import RichHelpFormatter


class DwiParticipantsSessionsWorkflow(BaseWorkflow):
    def __init__(self, seed, **kwargs):
        super().__init__(**kwargs, name="dwi_participants_sessions")
        self.seed = seed

        self.rng = np.random.default_rng(seed=self.seed)

    def run_main(self):

        data_for_df = []

        for dpath in Path(self.layout.dpath_bids).glob("*/*/dwi"):
            bids_session_id = dpath.parent.name
            bids_participant_id = dpath.parent.parent.name

            participant_id = check_participant_id(bids_participant_id)
            session_id = check_session_id(bids_session_id)

            data_for_df.append((participant_id, session_id))

        df = pd.DataFrame(data_for_df, columns=["participant_id", "session_id"])
        df = df.iloc[self.rng.permutation(len(df))]
        self.logger.info(f"\n{df}")

        fpath_out = "dwi_participants_sessions.csv"
        df.to_csv(fpath_out, index=False, header=False)
        self.logger.info(f"Saved to {fpath_out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Get list of participants and sessions that have dwi data."),
        formatter_class=RichHelpFormatter,
    )
    add_arg_dataset_root(parser)
    parser.add_argument("--seed", type=int, default=3791, help="Seed for RNG.")
    add_arg_verbosity(parser)

    args = parser.parse_args()
    workflow = DwiParticipantsSessionsWorkflow(
        dpath_root=args.dataset_root, seed=args.seed
    )
    workflow.logger.setLevel(VERBOSITY_TO_LOG_LEVEL_MAP[args.verbosity])

    # capture warnings
    logging.captureWarnings(True)
    capture_warnings(workflow.logger)

    try:
        workflow.run()
    except Exception:
        workflow.logger.exception("An error occurred while running the workflow.")
