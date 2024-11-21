#!/usr/bin/env python

import argparse
import logging
from pathlib import Path

import pandas as pd
from nipoppy.cli.parser import add_arg_dataset_root
from nipoppy.logger import add_logfile, capture_warnings
from nipoppy.tabular import Bagel, Manifest
from nipoppy.utils import (
    participant_id_to_bids_participant_id,
    save_df_with_backup,
    session_id_to_bids_session_id,
)
from nipoppy.workflows import BaseWorkflow

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.tabular_filters import loading_func
from nipoppy_ppmi.tabular_utils import get_tabular_info_and_merge

DASH_BAGEL_COL_BIDS_PARTICIPANT_ID = "bids_id"
DASH_BAGEL_COL_PARTICIPANT_ID = Manifest.col_participant_id
DASH_BAGEL_COL_SESSION_ID = "session"
DASH_BAGEL_ID_COLS = {
    Bagel.col_bids_participant_id: DASH_BAGEL_COL_BIDS_PARTICIPANT_ID,
    Manifest.col_participant_id: DASH_BAGEL_COL_PARTICIPANT_ID,
    Manifest.col_visit_id: DASH_BAGEL_COL_SESSION_ID,
}
DASH_BAGEL_VAR_NAME = "assessment_name"
DASH_BAGEL_VAR_VALUE = "assessment_score"


class GenerateTabularBagelWorkflow(BaseWorkflow):

    def __init__(self, **kwargs):
        super().__init__(name=Path(__file__).stem, **kwargs)

    def run_main(self):

        custom_config = CustomConfig(**self.config.CUSTOM)

        # combine demographics info
        df_demographics = get_tabular_info_and_merge(
            custom_config.DEMOGRAPHICS,
            df_manifest=self.manifest,
            visits=self.config.VISIT_IDS,
            loading_func=loading_func,
            logger=self.logger,
        )

        self.logger.info(f"Generated demographics dataframe: {df_demographics.shape}")
        self.logger.debug(f"\n{df_demographics}")

        df_assessments = get_tabular_info_and_merge(
            custom_config.ASSESSMENTS,
            df_manifest=self.manifest,
            visits=self.config.VISIT_IDS,
            loading_func=loading_func,
            logger=self.logger,
        )

        self.logger.info(f"Generated assessments dataframe: {df_assessments.shape}")
        self.logger.debug(f"\n{df_assessments}")

        # combine everything into a single file
        df_bagel = df_demographics.merge(
            df_assessments, on=Manifest.index_cols, how="outer"
        )
        df_bagel = df_bagel.drop_duplicates().reset_index(drop=True)
        df_bagel.insert(
            1,
            Bagel.col_bids_participant_id,
            df_bagel[Manifest.col_participant_id].apply(
                participant_id_to_bids_participant_id
            ),
        )
        self.logger.info(f"Generated bagel: {df_bagel.shape}")
        self.logger.debug(f"\n{df_bagel}")

        # check that the bagel has no duplicate entries
        idx_bagel = df_bagel.loc[:, Manifest.index_cols].apply(
            lambda df: " ".join(df.dropna().astype(str).values), axis=1
        )
        idx_bagel_counts = idx_bagel.value_counts()
        has_duplicates = idx_bagel_counts.loc[idx_bagel_counts > 1]
        if len(has_duplicates) > 0:
            df_bagel_duplicates = df_bagel.loc[idx_bagel.isin(has_duplicates.index)]
            df_bagel_duplicates.to_csv("bagel_duplicates.csv", index=False)
            raise RuntimeError(
                f"Bagel has duplicate entries (saved to bagel_duplicates.csv):\n{df_bagel_duplicates}"
            )

        fpath_bagel_with_timestamp = save_df_with_backup(
            df_bagel, self.layout.dpath_tabular / "tabular_bagel.tsv"
        )
        if fpath_bagel_with_timestamp is not None:
            self.logger.info(f"Saved bagel to {fpath_bagel_with_timestamp}")
        else:
            self.logger.info("No changes to bagel file. Will not write new file.")

        # make and save dashboard bagel
        df_dash_bagel = pd.melt(
            df_bagel,
            id_vars=DASH_BAGEL_ID_COLS.keys(),
            var_name=DASH_BAGEL_VAR_NAME,
            value_name=DASH_BAGEL_VAR_VALUE,
        )
        df_dash_bagel = df_dash_bagel.rename(columns=DASH_BAGEL_ID_COLS)
        df_dash_bagel[DASH_BAGEL_COL_SESSION_ID] = df_dash_bagel[
            DASH_BAGEL_COL_SESSION_ID
        ].apply(session_id_to_bids_session_id)
        fpath_dash_bagel_with_timestamp = save_df_with_backup(
            df_dash_bagel,
            self.layout.dpath_tabular / "dashboard_bagel.csv",
            sep=",",
        )
        if fpath_dash_bagel_with_timestamp is not None:
            self.logger.info(
                f"Saved dashboard bagel to {fpath_dash_bagel_with_timestamp}"
            )
        else:
            self.logger.info(
                "No changes to dashboard bagel file. Will not write new file."
            )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Process/aggregate tabular data and create a single bagel file for "
            "tracking tabular data availability."
        )
    )
    add_arg_dataset_root(parser)
    args = parser.parse_args()

    workflow = GenerateTabularBagelWorkflow(
        dpath_root=args.dataset_root,
    )
    # workflow.logger.setLevel(logging.DEBUG)
    add_logfile(workflow.logger, workflow.generate_fpath_log())

    # capture warnings
    logging.captureWarnings(True)
    capture_warnings(workflow.logger)

    try:
        workflow.run()
    except Exception as e:
        workflow.logger.exception(
            "An error occurred while generating the tabular bagel file"
        )
