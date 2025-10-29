#!/usr/bin/env python

import argparse
import json
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
from nipoppy.logger import add_logfile, capture_warnings
from nipoppy.tabular import Manifest
from nipoppy.tabular.dicom_dir_map import DicomDirMap
from nipoppy.utils import session_id_to_bids_session_id
from nipoppy.workflows import BaseWorkflow
from rich_argparse import RichHelpFormatter

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.env import (
    COL_GROUP_TABULAR,
    COL_NEUROMELANIN_MANIFEST,
    COL_SUBJECT_TABULAR,
    COL_VISIT_TABULAR,
    DATATYPE_ANAT,
    DATATYPE_DWI,
    DATATYPE_FUNC,
)
from nipoppy_ppmi.heuristic import RE_NEUROMELANIN
from nipoppy_ppmi.tabular_filters import loading_func
from nipoppy_ppmi.imaging_utils import get_all_descriptions
from nipoppy_ppmi.tabular_utils import get_tabular_info, load_and_process_df_imaging

# subject groups to keep
GROUPS_KEEP = ["Parkinson's Disease", "Prodromal", "Healthy Control", "SWEDD"]

COLS_MANIFEST = list(Manifest().columns) + [COL_NEUROMELANIN_MANIFEST]

DATATYPES = [DATATYPE_ANAT, DATATYPE_DWI, DATATYPE_FUNC]

# flags
FLAG_REGENERATE = "--regenerate"


def _get_datatype_list(descriptions: pd.Series, description_datatype_map, seen=None):

    datatypes = descriptions.map(description_datatype_map)
    datatypes = datatypes.loc[~datatypes.isna()]
    datatypes = datatypes.drop_duplicates().sort_values().to_list()

    if isinstance(seen, set):
        seen.update(datatypes)

    return datatypes


class ManifestWorkflow(BaseWorkflow):

    def __init__(
        self,
        regenerate: bool = False,
        **kwargs,
    ):
        super().__init__(name=Path(__file__).stem, **kwargs)
        self.regenerate = regenerate

    def run_main(self):

        # parse global config
        visits = self.config.VISIT_IDS
        expected_sessions = self.config.SESSION_IDS

        custom_config = CustomConfig(**self.config.CUSTOM)

        # generate filepaths
        fpaths_demographics = list(
            {file_info.FILEPATH for file_info in custom_config.DEMOGRAPHICS.values()}
        )
        fpaths_assessments = list(
            {file_info.FILEPATH for file_info in custom_config.ASSESSMENTS.values()}
        )
        fpath_imaging = custom_config.IMAGING_INFO.FILEPATH
        fpath_group = custom_config.DEMOGRAPHICS["COHORT_DEFINITION"].FILEPATH
        fpath_descriptions = custom_config.IMAGE_DESCRIPTIONS.FILEPATH

        for fpath in [
            fpath_imaging,
            *fpaths_demographics,
            *fpaths_assessments,
            fpath_group,
            fpath_descriptions,
        ]:
            if not fpath.exists():
                raise RuntimeError(f"File {fpath} does not exist")

        # load data dfs and heuristics json
        df_imaging = load_and_process_df_imaging(fpath_imaging)
        df_group = pd.read_csv(fpath_group, dtype=str)

        # this is a hack to get static and non-static
        # data combining demographic and assessment data
        tabular_info_dict = {}
        for tabular_file_group in [
            custom_config.DEMOGRAPHICS,
            custom_config.ASSESSMENTS,
        ]:
            for col_name, info in tabular_file_group.items():
                if col_name in tabular_info_dict:
                    raise RuntimeError(
                        f"Tabular column name {col_name} is duplicated in the global configs file"
                    )
                tabular_info_dict[col_name] = info

        df_static, df_nonstatic = get_tabular_info(
            tabular_info_dict,
            visits=visits,
            loading_func=loading_func,
            logger=self.logger,
        )

        with fpath_descriptions.open("r") as file_descriptions:
            datatype_descriptions_map: dict = json.load(file_descriptions)

        # reverse the mapping
        description_datatype_map = {}
        for datatype in DATATYPES:
            descriptions = get_all_descriptions(datatype_descriptions_map[datatype])
            for description in descriptions:
                if description in description_datatype_map:
                    self.logger.warning(
                        f'Description {description} has more than one associated datatype, using "{description_datatype_map[description]}"'
                    )
                else:
                    description_datatype_map[description] = datatype

        # ===== format tabular data =====

        # rename columns
        df_nonstatic = df_nonstatic.rename(
            columns={
                COL_SUBJECT_TABULAR: Manifest.col_participant_id,
                COL_VISIT_TABULAR: Manifest.col_visit_id,
            }
        )
        df_group = df_group.rename(
            columns={
                COL_SUBJECT_TABULAR: Manifest.col_participant_id,
            }
        )
        df_static = df_static.rename(
            columns={
                COL_SUBJECT_TABULAR: Manifest.col_participant_id,
            }
        )

        # add group info to tabular dataframe
        df_nonstatic = df_nonstatic.merge(
            df_group[[Manifest.col_participant_id, COL_GROUP_TABULAR]],
            on=Manifest.col_participant_id,
            how="left",
        )
        if df_nonstatic[COL_GROUP_TABULAR].isna().any():

            df_tabular_missing_group = df_nonstatic.loc[
                df_nonstatic[COL_GROUP_TABULAR].isna(),
                Manifest.col_participant_id,
            ]
            self.logger.warning(
                "\nSome subjects in tabular data do not belong to any research group"
                f"\n{df_tabular_missing_group}"
            )

            # try to find group in imaging dataframe
            for idx, subject in df_tabular_missing_group.items():

                group = df_imaging.loc[
                    df_imaging[Manifest.col_participant_id] == subject,
                    COL_GROUP_TABULAR,
                ].drop_duplicates()

                try:
                    group = group.item()
                except ValueError:
                    continue

                df_nonstatic.loc[idx, COL_GROUP_TABULAR] = group

            if df_nonstatic[COL_GROUP_TABULAR].isna().any():
                self.logger.warning(
                    "\nDid not successfully fill in missing group values using imaging data"
                    f"\n{df_nonstatic.loc[df_nonstatic[COL_GROUP_TABULAR].isna()]}"
                )

            else:
                self.logger.info(
                    "Successfully filled in missing group values using imaging data"
                )

        # ===== process imaging data =====

        self.logger.info(f"Processing imaging data...\tShape: {df_imaging.shape}")
        self.logger.debug(
            "\nSession counts:"
            f"\n{df_imaging[Manifest.col_session_id].value_counts(dropna=False)}"
        )

        # check if all expected sessions are present
        diff_sessions = set(expected_sessions) - set(
            df_imaging[Manifest.col_session_id]
        )
        if len(diff_sessions) != 0:
            self.logger.warning(
                f"Did not encounter all sessions listed in global_config. Missing: {diff_sessions}"
            )

        # only keep sessions that are listed in global_config
        n_img_before_session_drop = df_imaging.shape[0]
        df_imaging = df_imaging.loc[
            df_imaging[Manifest.col_session_id].isin(expected_sessions)
        ]
        self.logger.info(
            f"Dropped {n_img_before_session_drop - df_imaging.shape[0]} imaging entries"
            f" because the session was not in {expected_sessions}"
        )
        self.logger.debug(
            "\nCohort composition:"
            f"\n{df_imaging[COL_GROUP_TABULAR].value_counts(dropna=False)}"
        )

        # check if all expected groups are present
        diff_groups = set(GROUPS_KEEP) - set(df_imaging[COL_GROUP_TABULAR])
        if len(diff_groups) != 0:
            self.logger.warning(
                f"Did not encounter all groups listed in GROUPS_KEEP. Missing: {diff_groups}"
            )

        # only keep subjects in certain groups
        n_img_before_subject_drop = df_imaging.shape[0]
        df_imaging = df_imaging.loc[df_imaging[COL_GROUP_TABULAR].isin(GROUPS_KEEP)]
        self.logger.info(
            f"Dropped {n_img_before_subject_drop - df_imaging.shape[0]} imaging entries"
            f" because the subject's research group was not in {GROUPS_KEEP}"
        )

        # create imaging datatype availability lists
        seen_datatypes = set()
        df_imaging[COL_NEUROMELANIN_MANIFEST] = df_imaging[Manifest.col_datatype]
        df_imaging = df_imaging.groupby(
            [
                Manifest.col_participant_id,
                Manifest.col_visit_id,
                Manifest.col_session_id,
            ]
        )[[Manifest.col_datatype, COL_NEUROMELANIN_MANIFEST]].aggregate(
            {
                Manifest.col_datatype: lambda descriptions: _get_datatype_list(
                    descriptions, description_datatype_map, seen=seen_datatypes
                ),
                COL_NEUROMELANIN_MANIFEST: lambda descriptions: any(
                    [
                        re.search(RE_NEUROMELANIN, description)
                        for description in descriptions
                    ]
                ),
            }
        )
        df_imaging = df_imaging.reset_index()
        self.logger.info(f"Final imaging dataframe shape: {df_imaging.shape}")

        # check if all expected datatypes are present
        diff_datatypes = set(DATATYPES) - seen_datatypes
        if len(diff_datatypes) != 0:
            self.logger.warning(
                f"Did not encounter all datatypes in datatype_descriptions_map. Missing: {diff_datatypes}"
            )

        self.logger.info("Processing tabular data..." f"\tShape: {df_nonstatic.shape}")
        self.logger.debug(
            "\nCohort composition:"
            f"\n{df_nonstatic[COL_GROUP_TABULAR].value_counts(dropna=False)}"
        )

        # only keep subjects in certain groups
        n_tab_before_subject_drop = df_nonstatic.shape[0]
        df_nonstatic = df_nonstatic.loc[
            df_nonstatic[COL_GROUP_TABULAR].isin(GROUPS_KEEP)
        ]
        self.logger.info(
            f"Dropped {n_tab_before_subject_drop - df_nonstatic.shape[0]} tabular entries"
            f" because the subject's research group was not in {GROUPS_KEEP}"
        )

        # merge on subject and visit
        key_merge = "_merge"
        df_manifest = df_nonstatic.merge(
            df_imaging,
            how="outer",
            on=[Manifest.col_participant_id, Manifest.col_visit_id],
            indicator=key_merge,
        )

        # warning if missing tabular information
        subjects_without_demographic = set(
            df_manifest[Manifest.col_participant_id]
        ) - set(df_nonstatic[Manifest.col_participant_id])
        if len(subjects_without_demographic) > 0:
            self.logger.warning(
                "Some subjects have imaging data but no demographic information "
                f"{subjects_without_demographic}, dropping them from the manifest"
            )
        df_manifest = df_manifest.loc[
            ~df_manifest[Manifest.col_participant_id].isin(subjects_without_demographic)
        ]

        # replace NA datatype by empty list
        df_manifest[Manifest.col_datatype] = df_manifest[Manifest.col_datatype].apply(
            lambda datatype: datatype if isinstance(datatype, list) else []
        )

        # replace NA neuromelanin by False
        df_manifest[COL_NEUROMELANIN_MANIFEST] = (
            df_manifest[COL_NEUROMELANIN_MANIFEST]
            .astype(float)
            .fillna(False)
            .astype(bool)
            .astype(str)
        )

        # populate other columns
        for col in COLS_MANIFEST:
            if not (col in df_manifest.columns):
                df_manifest[col] = np.nan

        # only keep new subject/session pairs
        # otherwise we build/rebuild the manifest from scratch
        if (not self.regenerate) and (self.layout.fpath_manifest.exists()):

            df_manifest_old = Manifest()
            try:
                df_manifest_old = self.manifest
            except Exception as exception:
                self.logger.warning(
                    f"Could not load old manifest at {self.layout.fpath_manifest}: {exception}"
                )

            subject_session_pairs_old = pd.Index(
                zip(
                    df_manifest_old[Manifest.col_participant_id],
                    df_manifest_old[Manifest.col_session_id],
                )
            )

            df_manifest = df_manifest.set_index(
                [Manifest.col_participant_id, Manifest.col_session_id]
            )

            # error if new manifest loses subject-session pairs
            df_manifest_deleted_rows = df_manifest_old.loc[
                ~subject_session_pairs_old.isin(df_manifest.index)
            ]
            if len(df_manifest_deleted_rows) > 0:
                raise RuntimeError(
                    "Some of the subject/session pairs in the old manifest do not"
                    " seem to exist anymore:"
                    f"\n{df_manifest_deleted_rows}"
                    f"\nUse {FLAG_REGENERATE} to fully regenerate the manifest"
                )

            df_manifest_new_rows = df_manifest.loc[
                ~df_manifest.index.isin(subject_session_pairs_old)
            ]
            df_manifest_new_rows = df_manifest_new_rows.reset_index()[COLS_MANIFEST]
            df_manifest = pd.concat(
                [df_manifest_old, df_manifest_new_rows], axis="index"
            )
            if len(df_manifest_old) != 0:
                self.logger.info(
                    f"Added {len(df_manifest_new_rows)} rows to existing manifest"
                )

        # reorder columns and sort
        df_manifest = df_manifest[COLS_MANIFEST]
        # # sorting not working because save_tabular_file forces a sort
        # df_manifest = df_manifest.sort_values(
        #     by=Manifest.col_visit_id,
        #     key=(lambda visits: visits.apply(self.config.VISIT_IDS.index)),
        # )
        # df_manifest = df_manifest.sort_values(
        #     by=Manifest.col_participant_id, kind="stable"
        # ).reset_index(drop=True)

        # drop duplicates (based on df cast as string)
        df_manifest = df_manifest.loc[df_manifest.astype(str).drop_duplicates().index]

        # validate
        df_manifest: Manifest = Manifest(df_manifest).validate().sort_values()

        self.logger.info("\nCreated manifest:" f"\n{df_manifest}")
        self.save_tabular_file(df_manifest, self.layout.fpath_manifest)

        # also create the participant dicom dir
        # add dicom directory information
        if self.config.DICOM_DIR_MAP_FILE is None:
            raise RuntimeError("DICOM_DIR_MAP_FILE not specified in the global config")
        df_dicom_dir_map = df_manifest.get_imaging_subset()
        df_dicom_dir_map[DicomDirMap.col_participant_dicom_dir] = (
            df_dicom_dir_map.apply(
                lambda df: (
                    f"{session_id_to_bids_session_id(df[Manifest.col_session_id])}/{df[Manifest.col_participant_id]}"
                    if not pd.isna(df[Manifest.col_session_id])
                    else np.nan
                ),
                axis="columns",
            )
        )
        df_dicom_dir_map = DicomDirMap(df_dicom_dir_map).validate()
        self.save_tabular_file(df_dicom_dir_map, self.config.DICOM_DIR_MAP_FILE)


if __name__ == "__main__":
    # argparse
    HELPTEXT = f"""
    Script to generate manifest file for PPMI dataset.
    Requires an imaging data availability info file that can be downloaded from 
    the LONI IDA, as well as the demographic information CSV file from the PPMI website.
    The name of these files should be specified in the global config file.
    """
    parser = argparse.ArgumentParser(
        description=HELPTEXT,
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "dataset_root",
        type=Path,
    )
    parser.add_argument(
        FLAG_REGENERATE,
        action="store_true",
        help=(
            "regenerate entire manifest"
            " (default: only append rows for new subjects/sessions)"
        ),
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="do not actually save any files",
    )
    args = parser.parse_args()

    workflow = ManifestWorkflow(
        dpath_root=args.dataset_root,
        regenerate=args.regenerate,
        dry_run=args.dry_run,
    )
    workflow.logger.setLevel(logging.DEBUG)
    add_logfile(workflow.logger, workflow.generate_fpath_log())

    # capture warnings
    logging.captureWarnings(True)
    capture_warnings(workflow.logger)

    try:
        workflow.run()
    except Exception as e:
        workflow.logger.exception("An error occurred while generating the manifest")
