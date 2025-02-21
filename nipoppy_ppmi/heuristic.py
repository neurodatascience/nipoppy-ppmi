#!/usr/bin/env python
import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Sequence

import pandas as pd  # part of Heudiconv container

# BIDS standard
DATATYPE_ANAT = "anat"
DATATYPE_DWI = "dwi"
SUFFIX_T1 = "T1w"
SUFFIX_T2 = "T2w"
SUFFIX_T2_STAR = "T2starw"
SUFFIX_FLAIR = "FLAIR"
SUFFIX_DWI = "dwi"

DPATH_CURRENT = Path(__file__).parent

# datatype/suffix to description mapping (from nipoppy-ppmi script)
# this file needs to be available in the container
FPATH_DESCRIPTIONS = (
    DPATH_CURRENT.parent / "imaging_descriptions" / "ppmi_imaging_descriptions.json"
)

# LONI IDA Search result file
DPATH_DATASET_ROOT = DPATH_CURRENT.parent.parent
FPATH_IMAGING = DPATH_DATASET_ROOT / "sourcedata" / "tabular" / "idaSearch.csv"
COL_PROTOCOL = "Imaging Protocol"
COL_IMAGE_ID = "Image ID"
COL_MODALITY = "Modality"
MODALITY_DWI = "DTI"
RE_IMAGE_ID = ".*_I([0-9]+).dcm"  # regex
RE_NEUROMELANIN = "([nN][mM])|([gG][rR][eE].*[mM][tT])"  # neuromelanin pattern
SEP_PROTOCOL_INFO_ENTRY = ";"
SEP_PROTOCOL_INFO = "="
KEY_PLANE = "Acquisition Plane"
KEY_DIMS = "Acquisition Type"

# dwi directions
VALID_DIRS = ["AP", "PA", "LR", "RL"]
DIR_RE_MAP = {
    # will catch: ' R L', '_RL', 'R-L', 'R > L', etc.
    dir: f"[ \-_]{dir[0]}[ \-_>]*{dir[1]}(?:[ \-_]|\Z)"
    for dir in VALID_DIRS
}
# for descriptions not handled by the above regex
DIR_DESCRIPTIONS_MAP = {
    "LR": [
        "2D DTI EPI FAT SHIFT LEFT",
        "AX DTI 32 DIR FAT SHIFT L",
        "AX DTI 32 DIR FAT SHIFT L NO ANGLE",
        "AX DTI _reverse",  # one subject has this and 'AX DTI _RL'
    ],
    "RL": [
        "2D DTI EPI FAT SHIFT RIGHT",
        "AX DTI 32 DIR FAT SHIFT R",
        "AX DTI 32 DIR FAT SHIFT R NO ANGLE",
    ],
}

# dwi acquisitions (for AP/PA scans)
DESCRIPTION_ACQ_MAP = {
    "DTI_B0_PA": "B0",
    "DTI_revB0_AP": "B0",
    "DTI_B700_64dir_PA": "B700",
    "DTI_B1000_64dir_PA": "B1000",
    "DTI_B2000_64dir_PA": "B2000",
}

# acq tags for anatomical scans
TAG_NEUROMELANIN = "NM"
TAG_SAG = "sag"
TAG_COR = "cor"
TAG_AX = "ax"
TAG_2D = "2D"
TAG_3D = "3D"
MAP_PLANE = {"SAGITTAL": TAG_SAG, "CORONAL": TAG_COR, "AXIAL": TAG_AX}
MAP_DIMS = {"2D": TAG_2D, "3D": TAG_3D}
TAGS_PLANE = [TAG_SAG, TAG_COR, TAG_AX]
TAGS_DIMS = [TAG_2D, TAG_3D]

# format for runs
PATTERN_ITEM = "{item:02d}"

HEURISTIC_HELPER = None


def infotodict(seqinfo, heuristic_helper=None, testing=False):
    """Heuristic evaluator for determining which runs belong where
    allowed template fields - follow python string module:
    item: index within category
    subject: participant id
    session: session id (including 'ses-' prefix)
    """

    global HEURISTIC_HELPER

    HEURISTIC_HELPER = heuristic_helper

    if HEURISTIC_HELPER is None:
        print("initializing HeuristicHelper in heuristic")
        HEURISTIC_HELPER = HeuristicHelper()

    info = defaultdict(list)
    for _, s in enumerate(seqinfo):

        image_id = get_image_id_from_dcm(s.example_dcm_file)

        # append image ID instead of series description if testing
        # easier to debug/check in LONI
        if testing:
            to_append = image_id
        else:
            to_append = s.series_id  # for actual Heudiconv run

        # hardcoded, hard-to-handle cases
        # T1 sagittal 3D
        # - 4 image with ambiguous descriptions:  "MRI MAGNETIC RESONANCE EXAM", "PPMI 2.0", "MR"
        # - sT1W_3D_TFE have "DTI" modality
        # - 3 images with "MPRAGE_ASO" description but parsed as NA in Heudiconv
        if image_id in [
            "1609526",
            "1680311",
            "1196642",
            "1119726",
            "1120679",
            "1397807",
            "1397808",
        ] or s.series_description in ["sT1W_3D_TFE"]:
            info[create_key_anat(SUFFIX_T1, plane=TAG_SAG, dims=TAG_3D)].append(
                to_append
            )
            continue
        # FLAIR sagittal 3D
        elif image_id == "1609534":
            info[create_key_anat(SUFFIX_FLAIR, plane=TAG_SAG, dims=TAG_3D)].append(
                to_append
            )
            continue
        # T2 axial 2D
        elif image_id == "1397805":
            info[create_key_anat(SUFFIX_T2, plane=TAG_AX, dims=TAG_2D)].append(
                to_append
            )
            continue
        # generic diffusion
        elif image_id in ["1680316", "1680317"]:
            info[create_key_dwi()].append(to_append)
            continue

        try:
            # suffix could be None
            datatype, suffix = HEURISTIC_HELPER.get_datatype_suffix_from_description(
                s.series_description
            )

            imaging_protocol_info_str = HEURISTIC_HELPER.df_imaging.loc[
                image_id, COL_PROTOCOL
            ]
            imaging_protocol_info_parsed = {}
            if not pd.isna(imaging_protocol_info_str):
                for protocol_info_entry in imaging_protocol_info_str.split(
                    SEP_PROTOCOL_INFO_ENTRY
                ):
                    key, value = protocol_info_entry.split(SEP_PROTOCOL_INFO)
                    imaging_protocol_info_parsed[key] = value

            modality = HEURISTIC_HELPER.df_imaging.loc[image_id, COL_MODALITY]

            plane = None  # to fill in
            dims = None
            if datatype == DATATYPE_ANAT:

                if re.search(RE_NEUROMELANIN, s.series_description):
                    info[create_key_anat(suffix, acq=TAG_NEUROMELANIN)].append(
                        to_append
                    )

                else:

                    # image dimensions: 2D or 3D
                    try:
                        dims = MAP_DIMS[imaging_protocol_info_parsed[KEY_DIMS]]
                    except KeyError:
                        for tag_dim in TAGS_DIMS:
                            if tag_dim.lower() in s.series_description.lower():
                                if dims is not None:
                                    raise RuntimeError(
                                        f"Found multiple dims tags in description: {s.series_description}"
                                    )
                                dims = tag_dim

                    # acquisition plane: sagittal, coronal, or axial
                    try:
                        plane = MAP_PLANE[imaging_protocol_info_parsed[KEY_PLANE]]
                    except KeyError:
                        for tag_plane in TAGS_PLANE:
                            if tag_plane.lower() in s.series_description.lower():
                                if plane is not None:
                                    raise RuntimeError(
                                        f"Found multiple plane tags in description: {s.series_description}"
                                    )
                                plane = tag_plane

                    if (
                        (dims is None)
                        or (plane is None)
                        and (modality == MODALITY_DWI)
                        and (s.series_description == "T1")
                        and (s.series_files) in [133, 184]
                    ):
                        dims = TAG_3D
                        plane = TAG_SAG

                    info[create_key_anat(suffix, plane=plane, dims=dims)].append(
                        to_append
                    )

            elif datatype == DATATYPE_DWI:
                acq = get_dwi_acq_from_description(s.series_description)
                dir = get_dwi_dir_from_description(s.series_description)
                info[create_key_dwi(dir=dir, acq=acq)].append(to_append)

            else:
                raise NotImplementedError(f"Not implemented for datatype {datatype}")

        except Exception as exception:
            exception_message = (
                f"ERROR in heuristic: {str(exception)} (image ID: {image_id})"
            )
            if testing:
                raise RuntimeError(exception_message)
            else:
                print(exception_message)
                continue

    return info


def create_key_anat(suffix, plane=None, dims=None, acq=None):

    if (acq is not None) and (plane is not None or dims is not None):
        raise RuntimeError("Cannot specify both acq and plane/dims")

    if (plane is not None) and (dims is not None):
        stem = (
            f"sub-{{subject}}_{{session}}_acq-{plane}{dims}_run-{PATTERN_ITEM}_{suffix}"
        )
    elif acq is not None:
        stem = f"sub-{{subject}}_{{session}}_acq-{acq}_run-{PATTERN_ITEM}_{suffix}"
    else:
        stem = f"sub-{{subject}}_{{session}}_run-{PATTERN_ITEM}_{suffix}"

    return create_key(DATATYPE_ANAT, stem)


def create_key_dwi(suffix=SUFFIX_DWI, dir=None, acq=None):

    if dir is not None:
        dir_tag = f"_dir-{dir}"
    else:
        dir_tag = ""

    if acq is not None:
        acq_tag = f"_acq-{acq}"
    else:
        acq_tag = ""

    stem = f"sub-{{subject}}_{{session}}{acq_tag}{dir_tag}_run-{PATTERN_ITEM}_{suffix}"
    return create_key(DATATYPE_DWI, stem)


def create_key(datatype, stem, outtype=("nii.gz",), annotation_classes=None):
    template = f"sub-{{subject}}/{{session}}/{datatype}/{stem}"
    return template, outtype, annotation_classes


def get_image_id_from_dcm(fname_dcm):
    match = re.match(RE_IMAGE_ID, fname_dcm)
    if not match:
        raise RuntimeError(f"Could not get image ID from {fname_dcm}")
    if len(match.groups()) > 1:
        raise RuntimeError(f"Got more than one image ID from {fname_dcm}")
    return match.group(1)


def get_dwi_acq_from_description(description: str):
    return DESCRIPTION_ACQ_MAP.get(description)  # returns None if not found


def get_dwi_dir_from_description(description: str):

    # check hardcoded description strings first
    for dir, descriptions in DIR_DESCRIPTIONS_MAP.items():
        if description in descriptions:
            return dir

    # then infer using regexes
    for dir, re_dir in DIR_RE_MAP.items():
        if re.search(re_dir, description):
            return dir

    # no direction found
    return None


class HeuristicHelper:

    datatypes = [DATATYPE_ANAT, DATATYPE_DWI]
    suffixes_anat = [SUFFIX_T1, SUFFIX_T2, SUFFIX_T2_STAR, SUFFIX_FLAIR]

    def __init__(self, fpath_imaging=None, fpath_descriptions=None) -> None:

        if fpath_imaging is None:
            fpath_imaging = FPATH_IMAGING
        if fpath_descriptions is None:
            fpath_descriptions = FPATH_DESCRIPTIONS

        self.fpath_imaging = Path(fpath_imaging)
        self.fpath_descriptions = Path(fpath_descriptions)

        if not self.fpath_imaging.exists():
            raise FileNotFoundError(
                f"Imaging info file {self.fpath_imaging} does not exist"
            )

        if not self.fpath_descriptions.exists():
            raise FileNotFoundError(
                f"Descriptions map file {self.fpath_descriptions} does not exist"
            )

        self.df_imaging = pd.read_csv(self.fpath_imaging, dtype=str).set_index(
            COL_IMAGE_ID
        )

        with open(self.fpath_descriptions, "r") as file_descriptions:
            self.descriptions_map = json.load(file_descriptions)

    def get_descriptions(self, keys) -> list[str]:

        keys_all = keys[:]

        descriptions = self.descriptions_map
        while len(keys) > 0:
            key = keys.pop(0)
            try:
                descriptions = descriptions[key]
            except KeyError:
                raise KeyError(f"Invalid keys: {keys_all} (error at key {key})")

        if not (
            (isinstance(descriptions, Sequence) and isinstance(descriptions[0], str))
        ):
            raise RuntimeError(
                f"Did not get expected format for descriptions: {descriptions} (keys: {keys_all})"
            )

        return descriptions

    def get_datatype_suffix_from_description(self, description: str):
        description = description.strip()
        for datatype in self.datatypes:
            if datatype == DATATYPE_ANAT:
                for suffix in self.suffixes_anat:
                    if description in self.get_descriptions([datatype, suffix]):
                        return datatype, suffix
            else:
                if description in self.get_descriptions([datatype]):
                    return datatype, None
                else:
                    raise RuntimeError(
                        f"Could not find datatype for description {description}"
                    )


if __name__ == "__main__":

    HELPTEXT = """
    Test HeuDiConv heuristic function (infotodict). Creates empty .nii.gz files
    with names determined by the heuristic function in an output directory. Note
    that this assumes that HeuDiConv will be run with the `-b`/`--bids` option!
    """

    from nipoppy.config import Config
    from nipoppy.layout import DatasetLayout
    from nipoppy.workflows.base import BaseWorkflow
    from nipoppy.utils import check_session_id

    from nipoppy_ppmi.custom_config import CustomConfig

    DEFAULT_DNAME_HEUDICONV = ".heudiconv"
    DEFAULT_DNAME_OUT = "fake_bids"

    class HeuristicCheckWorkflow(BaseWorkflow):
        def __init__(
            self, dpath_root, fname_heudiconv, session_id, dpath_out, **kwargs
        ):
            super().__init__(dpath_root=dpath_root, name="heuristic_test", **kwargs)
            self.fname_heudiconv = fname_heudiconv
            self.session_id = check_session_id(session_id)
            if dpath_out is not None:
                dpath_out = Path(dpath_out)
            self.dpath_out = dpath_out

        def run_main(self):
            custom_config = CustomConfig(**self.config.CUSTOM)

            heuristic_helper = HeuristicHelper(
                fpath_imaging=custom_config.IMAGING_INFO.FILEPATH,
                fpath_descriptions=custom_config.IMAGE_DESCRIPTIONS.FILEPATH,
            )

            dpath_heudiconv: Path = self.layout.dpath_bids / self.fname_heudiconv
            bids_session_id = f"ses-{self.session_id}"

            error_messages = []

            for dpath_subject in dpath_heudiconv.iterdir():

                subject = dpath_subject.name
                # stage1
                fpath_info = (
                    dpath_subject / "info" / f"dicominfo_ses-{self.session_id}.tsv"
                )
                # # stage2
                # fpath_info = (
                #     dpath_subject
                #     / f"ses-{self.session_id}"
                #     / "info"
                #     / f"dicominfo_ses-{self.session_id}.tsv"
                # )

                if not fpath_info.exists():
                    error_messages.append(
                        f"No info file found at {fpath_info} for subject {subject} session {self.session_id}"
                    )
                    continue

                print(f"===== {subject} =====")
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

                for key, value in info.items():
                    template: str = key[0]
                    print(f'{template}:\t{",".join(value)}')

                    if self.dpath_out is not None:

                        for i in range(len(value)):
                            fpath_out: Path = (
                                self.dpath_out
                                / template.format(
                                    subject=subject, session=bids_session_id, item=i + 1
                                )
                            ).with_suffix(".nii.gz")

                            fpath_out.parent.mkdir(parents=True, exist_ok=True)
                            fpath_out.touch()

            print("===== ERRORS =====")
            for error_message in error_messages:
                print(error_message)

    parser = argparse.ArgumentParser(description=HELPTEXT)

    parser.add_argument(
        "--dataset",
        type=Path,
        required=False,
        help=f"path to the dataset root",
    )
    parser.add_argument(
        "--session-id",
        type=str,
        required=True,
        help="session ID (without the BIDS prefix)",
    )
    parser.add_argument(
        "--heudiconv",
        default=DEFAULT_DNAME_HEUDICONV,
        type=str,
        help=f".heudiconv directory name ({DEFAULT_DNAME_HEUDICONV})",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_DNAME_OUT,
        type=str,
        help=f"output directory for dummy BIDS files (default: {DEFAULT_DNAME_OUT})",
    )

    args = parser.parse_args()

    workflow = HeuristicCheckWorkflow(
        dpath_root=args.dataset,
        fname_heudiconv=args.heudiconv,
        session_id=args.session_id,
        dpath_out=args.out,
    )
    workflow.run()
