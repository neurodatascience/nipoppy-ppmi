#!/usr/bin/env python

# this script can be run with a command like:
#   python get_participant_session_list.py --dataset-root /path/to/dataset --out-file /path/to/output.csv --protocol-step fmriprep
# then the output file will be a CSV containing participant and session IDs to run
# which can be used in the array job submission script

# to get more information about the script arguments, run:
#   python get_participant_session_list.py --help

import argparse
from functools import partial
from pathlib import Path
from typing import Optional

import pandas as pd
from rich_argparse import RichHelpFormatter

from nipoppy.cli.parser import add_arg_dataset_root
from nipoppy.logger import add_logfile
from nipoppy.workflows import (
    BaseWorkflow,
    BasePipelineWorkflow,
    BidsConversionRunner,
    PipelineRunner,
)

PROTOCOL_STEP_TO_WORKFLOW_MAP = {
    # bidsify does not need a pipeline name/version since it doesn't matter which
    # BIDS converter will be used
    "bidsify": partial(BidsConversionRunner, pipeline_name=""),
    # here is what this should look like for processing pipelines/versions
    # the keys can be modified to be something else if desired (this just means the --protocol-step)
    # but the pipeline_name/pipeline_version fields should match what is in the global config
    "fmriprep": partial(
        PipelineRunner, pipeline_name="fmriprep", pipeline_version="23.1.3"
    ),
    "mriqc": partial(PipelineRunner, pipeline_name="mriqc", pipeline_version="23.1.0"),
}
DEFAULT_PROTOCOL_STEP = list(PROTOCOL_STEP_TO_WORKFLOW_MAP.keys())[0]


class ParticipantSessionListWorkflow(BaseWorkflow):
    """Get a list of participants and their sessions."""

    def __init__(
        self,
        dpath_root: Path | str,
        fpath_out: str | Path,
        protocol_step: str,
        session_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            dpath_root=dpath_root,
            name=Path(__file__).stem,
            **kwargs,
        )
        self.protocol_step = protocol_step.lower()
        self.fpath_out = Path(fpath_out)
        self.session_id = session_id

    def run_main(self):
        try:
            workflow: BasePipelineWorkflow = PROTOCOL_STEP_TO_WORKFLOW_MAP[
                self.protocol_step
            ](
                dpath_root=self.dpath_root,
            )
        except KeyError:
            raise ValueError(f"Invalid protocol step: {self.protocol_step}")

        self.logger.info(f"Checking {workflow}")

        participants_sessions = workflow.get_participants_sessions_to_run(
            participant_id=None, session_id=self.session_id
        )

        df = pd.DataFrame(participants_sessions)
        self.logger.info(f"Generated participant/session list:\n{df}")

        self.mkdir(self.fpath_out.parent)
        df.to_csv(self.fpath_out, index=False, header=False)
        self.logger.info(f"Participant/session list written to {self.fpath_out}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate list with participants/sessions to run",
        formatter_class=RichHelpFormatter,
    )
    add_arg_dataset_root(parser)
    parser.add_argument(
        "--session-id",
        type=str,
        required=False,
        help="Session ID (optional)",
    )
    parser.add_argument(
        "--out-file",
        type=Path,
        required=True,
        help="Path to write the participant/session list",
    )
    parser.add_argument(
        "--protocol-step",
        type=str,
        choices=PROTOCOL_STEP_TO_WORKFLOW_MAP.keys(),
        default=DEFAULT_PROTOCOL_STEP,
        help=(f"Step in the protocol to perform (default: {DEFAULT_PROTOCOL_STEP})"),
    )
    args = parser.parse_args()

    workflow = ParticipantSessionListWorkflow(
        dpath_root=args.dataset_root,
        session_id=args.session_id,
        fpath_out=args.out_file,
        protocol_step=args.protocol_step,
    )

    logger = workflow.logger
    logger.setLevel("DEBUG")
    add_logfile(logger, workflow.generate_fpath_log())
    try:
        workflow.run()
    except Exception:
        logger.exception(
            "An error occurred while getting/writing the participant/session list"
        )
