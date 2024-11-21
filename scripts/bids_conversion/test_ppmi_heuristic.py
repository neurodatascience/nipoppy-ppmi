#!/usr/bin/env python
import argparse
from pathlib import Path

import pandas as pd
from nipoppy.cli.parser import add_arg_dataset_root
from nipoppy.logger import add_logfile
from nipoppy.utils import session_id_to_bids_session
from nipoppy.workflows import BaseWorkflow
from rich_argparse import RichHelpFormatter

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.heuristic import infotodict, HeuristicHelper


DPATH_CURRENT = Path(__file__).parent


class TestHeuristicWorkflow(BaseWorkflow):

    def __init__(
        self,
        session_id,
        dpath_out,
        fname_heudiconv=".heudiconv",
        **kwargs,
    ):
        super().__init__(name=Path(__file__).stem, **kwargs)
        self.session_id = session_id
        self.dpath_out = Path(dpath_out)
        self.fname_heudiconv = fname_heudiconv
        self.bids_session = session_id_to_bids_session(session_id)

    def run_main(self):

        custom_config = CustomConfig(**self.config.CUSTOM)

        heuristic_helper = HeuristicHelper(
            fpath_imaging=custom_config.IMAGING_INFO.FILEPATH,
            fpath_descriptions=custom_config.IMAGE_DESCRIPTIONS.FILEPATH,
        )

        dpath_heudiconv = self.layout.dpath_bids / self.fname_heudiconv

        error_messages = []

        for dpath_subject in dpath_heudiconv.iterdir():

            subject = dpath_subject.name
            fpath_info = (
                dpath_subject / "info" / f"dicominfo_ses-{self.bids_session}.tsv"
            )

            if not fpath_info.exists():
                error_messages.append(
                    f"No info file found for participant {subject}, session {self.session_id}: {fpath_info}"
                )
                continue

            # self.logger.info(f"===== {subject} =====")
            df_info = pd.read_csv(fpath_info, sep="\t", dtype=str)

            info = {}
            try:
                info = infotodict(
                    df_info.itertuples(index=False),
                    testing=True,
                    heuristic_helper=heuristic_helper,
                )
            except Exception as exception:
                error_messages.append(str(exception))
                # raise exception

            for key, value in info.items():
                template = key[0]
                self.logger.info(f'{template}:\t{",".join(value)}')

                if self.dpath_out is not None:

                    for i in range(len(value)):
                        fpath_out: Path = (
                            self.dpath_out
                            / template.format(
                                subject=subject, session=self.bids_session, item=i + 1
                            )
                        ).with_suffix(".nii.gz")

                        fpath_out.parent.mkdir(parents=True, exist_ok=True)
                        fpath_out.touch()

        self.logger.info("===== ERRORS =====")
        for error_message in error_messages:
            self.logger.error(error_message)


if __name__ == "__main__":

    HELPTEXT = """
    Dummy script to test filenames in DICOM-to-BIDS conversion (HeuDiConv).
    Run bids_conv --stage 1 first, then run this, check, and run bids_conv --stage 2.
    """

    parser = argparse.ArgumentParser(
        description=HELPTEXT, formatter_class=RichHelpFormatter
    )
    add_arg_dataset_root(parser)
    parser.add_argument("--session_id", type=str, required=True, help="session ID")
    parser.add_argument(
        "--heudiconv", default=".heudiconv", type=str, help=".heudiconv directory name"
    )
    parser.add_argument(
        "--out",
        default=DPATH_CURRENT / "fake_bids",
        type=Path,
        help="output directory for dummy BIDS files",
    )

    args = parser.parse_args()

    workflow = TestHeuristicWorkflow(
        dpath_root=args.dataset_root,
        session_id=args.session_id,
        dpath_out=args.out,
        fname_heudiconv=args.heudiconv,
    )

    add_logfile(workflow.logger, workflow.generate_fpath_log())

    try:
        workflow.run()
    except Exception:
        workflow.logger.exception("An error occurred with the test heuristic script")
