#!/usr/bin/env python

import argparse
import json
import logging
from pathlib import Path

import pandas as pd
from rich_argparse import RichHelpFormatter
from nipoppy.env import StrOrPathLike
from nipoppy.cli.parser import add_arg_dataset_root, add_arg_dry_run
from nipoppy.logger import add_logfile
from nipoppy.workflows import BaseWorkflow

from nipoppy_ppmi.custom_config import CustomConfig
from nipoppy_ppmi.env import (
    DATATYPE_ANAT,
    DATATYPE_DWI,
    DATATYPE_FUNC,
    DEFAULT_FNAME_IMAGING_DESCRIPTIONS,
    DEFAULT_FNAME_IMAGING_IGNORED,
    SUFFIX_T1,
    SUFFIX_T2,
    SUFFIX_T2_STAR,
    SUFFIX_FLAIR,
    COL_DESCRIPTION_IMAGING,
    COL_MODALITY_IMAGING,
    COL_PROTOCOL_IMAGING,
    MODALITY_ANAT,
    MODALITY_DWI,
    MODALITY_FUNC,
)
from nipoppy_ppmi.imaging_filters import EXCLUDE_IN_ANAT, EXCLUDE_IN_ANAT_T1, FILTERS
from nipoppy_ppmi.imaging_utils import get_all_descriptions

# mapping from BIDS datatype/suffix to PPMI "Modality" column
# the PPMI "Modality" column is not 100% accurate so we still have to check description strings
DATATYPE_MODALITY_MAP = {
    DATATYPE_DWI: MODALITY_DWI,
    DATATYPE_FUNC: MODALITY_FUNC,
    DATATYPE_ANAT: MODALITY_ANAT,
    SUFFIX_T1: MODALITY_ANAT,  # anat
    SUFFIX_T2: MODALITY_ANAT,  # anat
    SUFFIX_T2_STAR: MODALITY_ANAT,  # anat
    SUFFIX_FLAIR: MODALITY_ANAT,  # anat
}

FLAG_OVERWRITE = "--overwrite"
DEFAULT_DPATH_OUT = Path(__file__).parent.parent.parent / "imaging_descriptions"
DEFAULT_INDENT = 4


class FilterImageDescriptionsWorkflow(BaseWorkflow):

    def __init__(
        self,
        dpath_root: StrOrPathLike,
        dpath_out: StrOrPathLike,
        # fpath_imaging_relative: StrOrPathLike,
        fname_out_descriptions: StrOrPathLike = DEFAULT_FNAME_IMAGING_DESCRIPTIONS,
        fname_out_ignored: StrOrPathLike = DEFAULT_FNAME_IMAGING_IGNORED,
        indent: int = DEFAULT_INDENT,
        overwrite: bool = False,
        fpath_layout: StrOrPathLike | None = None,
        logger: logging.Logger | None = None,
        dry_run: bool = False,
    ):
        super().__init__(
            dpath_root=dpath_root,
            name="filter_image_descriptions",
            fpath_layout=fpath_layout,
            logger=logger,
            dry_run=dry_run,
        )
        # self.fpath_imaging_relative = Path(fpath_imaging_relative)
        self.dpath_out = Path(dpath_out)
        self.fname_out_descriptions = fname_out_descriptions
        self.fname_out_ignored = fname_out_ignored
        self.overwrite = overwrite
        self.indent = indent

    def _filter_descriptions(
        self,
        df: pd.DataFrame,
        datatype,
        common_substrings,
        reject_substrings=None,
        reject_substrings_exceptions=None,
        exclude_in=None,
        exclude_out=None,
        protocol_include=None,
        protocol_exclude=None,
    ):
        """Return a list of description strings corresponding to the datatype.

        Parameters
        ----------
        df : pd.DataFrame
            Input imaging data information
        datatype : str
            String for mapping to modality value
        common_substrings : list[str]
            Substrings commonly found in descriptions strings for this datatype
        reject_substrings : list[str] or None, optional
            Substrings in descriptions that should be rejected for this datatype, by default None
        reject_substrings_exceptions : list[str] or None, optional
            Exceptions for reject_substrings (e.g., 'T1 REPEAT2' has 'T2' but is a T1), by default None
        exclude_in : list[str] or None, optional
            Descriptions to exclude in within-modality search, by default None
        exclude_out : list[str] or None, optional
            Descriptions to exclude in out-of-modality search, by default None
        protocol_include : list[str] or None, optional
            Substrings required in imaging protocol column (e.g., Acquisition Type), by default None
        protocol_exclude : list[str] or None, optional
            Substrings to reject in imaging protocol column (e.g., Acquisition Type), by default None

        Returns
        -------
        list[str]
            The descriptions after all filtering operations
        """
        modality = DATATYPE_MODALITY_MAP[datatype]

        # filter based on imaging protocol column (e.g., Weighting, Acquisition Type)
        # rows that are excluded are completely rejected (not considered 'out-of-modality')
        protocol_filters_list = []
        if protocol_include is not None:
            df = df.loc[
                df[COL_PROTOCOL_IMAGING]
                .str.lower()
                .str.contains("|".join(protocol_include).lower(), na=False)
            ]
            protocol_filters_list.append(f'\t- WITH: {", ".join(protocol_include)}')
        if protocol_exclude is not None:
            df = df.loc[
                ~df[COL_PROTOCOL_IMAGING]
                .str.lower()
                .str.contains("|".join(protocol_exclude).lower(), na=False)
            ]
            protocol_filters_list.append(f'\t- WITHOUT: {", ".join(protocol_exclude)}')

        # initial set of descriptions
        df_modality = df.loc[df[COL_MODALITY_IMAGING] == modality]
        descriptions = df_modality[COL_DESCRIPTION_IMAGING].value_counts()
        self.logger.info(
            f"Found {len(descriptions)} unique description strings for modality {modality}"
        )
        for protocol_filters_str in protocol_filters_list:
            self.logger.debug(protocol_filters_str)

        # remove bad descriptions
        if exclude_in is not None and len(exclude_in) > 0:
            descriptions = descriptions.loc[~descriptions.index.isin(exclude_in)]
            if len(exclude_in) > 10:
                exclude_in_str = f"{len(exclude_in)} descriptions"
            else:
                exclude_in_str = f"{exclude_in}"
            self.logger.info(
                f"Got {len(descriptions)} unique descriptions after removing {exclude_in_str}"
            )

        # filter based on description strings (substring matching)
        if reject_substrings is not None and len(reject_substrings) > 0:

            if reject_substrings_exceptions is not None:
                descriptions_keep = descriptions.loc[
                    descriptions.index.str.lower().str.contains(
                        "|".join(reject_substrings_exceptions).lower()
                    )
                ]
                reject_substrings_exceptions_str = (
                    f" (except {reject_substrings_exceptions})"
                )
            else:
                descriptions_keep = None
                reject_substrings_exceptions_str = ""

            descriptions = descriptions.loc[
                ~descriptions.index.str.lower().str.contains(
                    "|".join(reject_substrings).lower()
                )
            ]

            descriptions = pd.concat([descriptions, descriptions_keep])
            descriptions = descriptions.loc[~descriptions.index.duplicated()]

            self.logger.info(
                f"Got {len(descriptions)} unique descriptions after removing those that contained one of {reject_substrings}{reject_substrings_exceptions_str}"
            )

        # find descriptions that don't contain a common substring
        suspicious_descriptions = descriptions.loc[
            ~descriptions.index.str.lower().str.contains(
                "|".join(common_substrings).lower()
            )
        ]
        if len(suspicious_descriptions) > 0:
            self.logger.warning(
                f"{len(suspicious_descriptions)} descriptions out of {len(descriptions)} do not contain any of {common_substrings}"
            )
            self.logger.warning(
                f"Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_in list"
            )
            self.logger.warning(f"\n{suspicious_descriptions}")

        # check other modalities
        df_other_modalities = df.loc[~df.index.isin(df_modality.index)]

        # remove known bad descriptions
        exclude_out_str = ""
        if exclude_out is not None and len(exclude_out) > 0:
            df_other_modalities = df_other_modalities.loc[
                ~df_other_modalities[COL_DESCRIPTION_IMAGING].isin(exclude_out)
            ]
            exclude_out_str = f" (after removing {exclude_out})"

        # check if any of the previously found descriptions are in another modality
        common_descriptions_other_modalities = df_other_modalities.loc[
            (df_other_modalities[COL_DESCRIPTION_IMAGING].isin(descriptions.index)),
            COL_DESCRIPTION_IMAGING,
        ].value_counts()
        if len(common_descriptions_other_modalities) > 0:
            self.logger.warning(
                f"Found {len(common_descriptions_other_modalities)}"
                " common descriptions with other modalities"
            )
            self.logger.warning(
                f"Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_out list"
            )
            self.logger.warning(f"\n{common_descriptions_other_modalities}")

        # find descriptions in other modalities that have a common substring
        descriptions_new = df_other_modalities.loc[
            (
                (
                    df_other_modalities[COL_DESCRIPTION_IMAGING]
                    .str.lower()
                    .str.contains("|".join(common_substrings).lower())
                )
                & (
                    ~df_other_modalities[COL_DESCRIPTION_IMAGING].isin(
                        descriptions.index
                    )
                )
            ),
            COL_DESCRIPTION_IMAGING,
        ].value_counts()
        if len(descriptions_new) > 0:
            self.logger.warning(
                f"Found {len(descriptions_new)} new description string(s) with one"
                f" of {common_substrings} in another modality{exclude_out_str}"
            )
            self.logger.warning(
                f"Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_out list"
            )
            self.logger.warning(f"\n{descriptions_new}")

        # combine
        descriptions = sorted(
            pd.concat([descriptions, descriptions_new])
            .index.drop_duplicates()
            .to_list()
        )

        return descriptions

    def run_main(self):

        # generate filepaths
        fpath_imaging = CustomConfig(**self.config.CUSTOM).IMAGING_INFO.FILEPATH
        fpath_out_descriptions = self.dpath_out / self.fname_out_descriptions
        fpath_out_ignored = self.dpath_out / self.fname_out_ignored

        # load df
        df_imaging = pd.read_csv(fpath_imaging)
        descriptions = {}

        # dwi
        self.logger.info(f"========== {DATATYPE_DWI} ==========".upper())
        descriptions[DATATYPE_DWI] = self._filter_descriptions(
            df=df_imaging,
            datatype=DATATYPE_DWI,
            **FILTERS[DATATYPE_DWI],
        )

        # func
        self.logger.info(f"========== {DATATYPE_FUNC} ==========".upper())
        descriptions[DATATYPE_FUNC] = self._filter_descriptions(
            df=df_imaging,
            datatype=DATATYPE_FUNC,
            **FILTERS[DATATYPE_FUNC],
        )

        # anat (dictionary with image subtypes)
        descriptions[DATATYPE_ANAT] = {}

        # t1
        self.logger.info(f"========== {DATATYPE_ANAT} ({SUFFIX_T1}) ==========".upper())
        descriptions[DATATYPE_ANAT][SUFFIX_T1] = self._filter_descriptions(
            df=df_imaging,
            datatype=SUFFIX_T1,
            exclude_in=EXCLUDE_IN_ANAT_T1,
            **FILTERS[DATATYPE_ANAT],
            **FILTERS[SUFFIX_T1],
        )

        # t2
        self.logger.info(f"========== {DATATYPE_ANAT} ({SUFFIX_T2}) ==========".upper())
        descriptions[DATATYPE_ANAT][SUFFIX_T2] = self._filter_descriptions(
            df=df_imaging,
            datatype=SUFFIX_T2,
            exclude_in=EXCLUDE_IN_ANAT + descriptions[DATATYPE_ANAT][SUFFIX_T1],
            **FILTERS[DATATYPE_ANAT],
            **FILTERS[SUFFIX_T2],
        )

        # t2 star
        self.logger.info(
            f"========== {DATATYPE_ANAT} ({SUFFIX_T2_STAR}) ==========".upper()
        )
        descriptions[DATATYPE_ANAT][SUFFIX_T2_STAR] = self._filter_descriptions(
            df=df_imaging,
            datatype=SUFFIX_T2_STAR,
            exclude_in=EXCLUDE_IN_ANAT
            + descriptions[DATATYPE_ANAT][SUFFIX_T1]
            + descriptions[DATATYPE_ANAT][SUFFIX_T2],
            **FILTERS[DATATYPE_ANAT],
            **FILTERS[SUFFIX_T2_STAR],
        )

        # flair
        self.logger.info(
            f"========== {DATATYPE_ANAT} ({SUFFIX_FLAIR}) ==========".upper()
        )
        descriptions[DATATYPE_ANAT][SUFFIX_FLAIR] = self._filter_descriptions(
            df=df_imaging,
            datatype=SUFFIX_FLAIR,
            exclude_in=EXCLUDE_IN_ANAT
            + descriptions[DATATYPE_ANAT][SUFFIX_T1]
            + descriptions[DATATYPE_ANAT][SUFFIX_T2],
            **FILTERS[DATATYPE_ANAT],
            **FILTERS[SUFFIX_FLAIR],
        )

        self.logger.debug(f"FINAL DESCRIPTIONS:")
        descriptions_all = get_all_descriptions(descriptions, logger=self.logger)

        # check if output file already exists
        for fpath in [fpath_out_descriptions, fpath_out_ignored]:
            if fpath.exists() and not self.overwrite:
                raise FileExistsError(
                    f"File exists: {fpath}. Use {FLAG_OVERWRITE} to overwrite"
                )

        # save
        if not self.dry_run:
            self.dpath_out.mkdir(parents=True, exist_ok=True)
            with fpath_out_descriptions.open("w") as file_out:
                json.dump(descriptions, file_out, indent=self.indent)
        self.logger.info(
            f"JSON file with datatype-descriptions mapping written to: {fpath_out_descriptions}"
        )

        # save another file with all images that didn't make it in any datatype
        df_ignored: pd.DataFrame = (
            df_imaging.loc[
                ~df_imaging[COL_DESCRIPTION_IMAGING].isin(descriptions_all),
                COL_DESCRIPTION_IMAGING,
            ]
            .drop_duplicates()
            .sort_values()
        )
        if not self.dry_run:
            df_ignored.to_csv(fpath_out_ignored, index=False)
        self.logger.info(f"Ignored descriptions written to: {fpath_out_ignored}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generate generate lists of protocol names for various datatypes and "
            "sub-datatypes. Also creates a CSV file with all images with a description "
            "not matching any datatype."
        ),
        formatter_class=RichHelpFormatter,
    )
    add_arg_dataset_root(parser)
    # parser.add_argument(
    #     "--fpath-imaging",
    #     type=Path,
    #     required=True,
    #     help=(
    #         "Path to the imaging data availability info file, relative to the "
    #         "dataset root (e.g., idaSearch.csv)"
    #     ),
    # )
    parser.add_argument(
        "--dpath-out",
        type=Path,
        default=DEFAULT_DPATH_OUT,
        help=(
            "Path to the directory where the output files are to be written"
            f" (default: {DEFAULT_DPATH_OUT})"
        ),
    )
    parser.add_argument(
        "--fname-out-descriptions",
        type=Path,
        default=DEFAULT_FNAME_IMAGING_DESCRIPTIONS,
        help=(
            "Path to the output JSON file with datatype-descriptions mapping"
            f" (default: {DEFAULT_FNAME_IMAGING_DESCRIPTIONS})"
        ),
    )
    parser.add_argument(
        "--fname-out-ignored",
        type=Path,
        default=DEFAULT_FNAME_IMAGING_IGNORED,
        help=(
            "Path to the output CSV file with ignored descriptions"
            f" (default: {DEFAULT_FNAME_IMAGING_IGNORED})"
        ),
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=DEFAULT_INDENT,
        help=(
            "Indentation level for the output JSON file with datatype-descriptions mapping"
            f" (default: {DEFAULT_INDENT})"
        ),
    )
    parser.add_argument(
        FLAG_OVERWRITE,
        action="store_true",
        help=(f"overwrite existing files"),
    )
    add_arg_dry_run(parser)
    args = parser.parse_args()

    workflow = FilterImageDescriptionsWorkflow(
        dpath_root=args.dataset_root,
        # fpath_imaging_relative=args.fpath_imaging,
        dpath_out=args.dpath_out,
        fname_out_descriptions=args.fname_out_descriptions,
        fname_out_ignored=args.fname_out_ignored,
        indent=args.indent,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )

    logger = workflow.logger
    logger.setLevel(logging.DEBUG)
    add_logfile(logger, workflow.generate_fpath_log())
    try:
        workflow.run()
    except Exception:
        logger.exception("An error occurred while generating the manifest file")
