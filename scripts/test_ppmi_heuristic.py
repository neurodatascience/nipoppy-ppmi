#!/usr/bin/env python
import argparse
import json

# import re
# from collections import defaultdict
from pathlib import Path

# from typing import Sequence

import pandas as pd

# from tabular.filters import DATATYPE_ANAT, DATATYPE_DWI, SUFFIX_T1, SUFFIX_T2, SUFFIX_T2_STAR, SUFFIX_FLAIR
from nipoppy.workflow.tabular.filter_image_descriptions import FNAME_DESCRIPTIONS

# from tabular.ppmi_utils import load_and_process_df_imaging
from nipoppy.workflow.bids_conv.heuristic import infotodict, HeuristicHelper

# from workflow.dicom_org.fetch_dicom_downloads import COL_IMAGE_ID

DPATH_CURRENT = Path(__file__).parent

FNAME_IMAGING = "idaSearch.csv"  # TODO use global configs
# COL_IMAGE_ID = 'Image ID' # TODO import from ppmi_utils.py

# # LONI IDA Search result file
# RE_IMAGE_ID = '.*_I([0-9]+).dcm' # regex
# RE_NEUROMELANIN = '[nN][mM]' # neuromelanin pattern
# SEP_PROTOCOL_INFO_ENTRY = ';'
# SEP_PROTOCOL_INFO = '='
# KEY_PLANE = 'Acquisition Plane'
# KEY_DIMS = 'Acquisition Type'

# # dwi directions
# VALID_DIRS = ['AP', 'PA', 'LR', 'RL']
# DIR_RE_MAP = {
#     # will catch: ' R L', '_RL', 'R-L', 'R > L', etc.
#     dir: f'[ \-_]{dir[0]}[ \-_>]*{dir[1]}(?:[ \-_]|\Z)'
#     for dir in VALID_DIRS
# }
# # for descriptions not handled by the above regex
# DIR_DESCRIPTIONS_MAP = {
#     'LR': [
#         '2D DTI EPI FAT SHIFT LEFT',
#         'AX DTI 32 DIR FAT SHIFT L',
#         'AX DTI 32 DIR FAT SHIFT L NO ANGLE',
#         'AX DTI _reverse', # one subject has this and 'AX DTI _RL'

#     ],
#     'RL': [
#         '2D DTI EPI FAT SHIFT RIGHT',
#         'AX DTI 32 DIR FAT SHIFT R',
#         'AX DTI 32 DIR FAT SHIFT R NO ANGLE'
#     ]
# }

# # dwi acquisitions (for AP/PA scans)
# DESCRIPTION_ACQ_MAP = {
#     'DTI_B0_PA': 'B0',
#     'DTI_revB0_AP': 'B0',
#     'DTI_B700_64dir_PA': 'B700',
#     'DTI_B1000_64dir_PA': 'B1000',
#     'DTI_B2000_64dir_PA': 'B2000',
# }

# TAG_NEUROMELANIN = 'NM'
# TAG_SAG = 'sag'
# TAG_COR = 'cor'
# TAG_AX = 'ax'
# TAG_2D = '2D'
# TAG_3D = '3D'
# MAP_PLANE = {'SAGITTAL': TAG_SAG, 'CORONAL': TAG_COR, 'AXIAL': TAG_AX}
# MAP_DIMS = {'2D': TAG_2D, '3D': TAG_3D}
# PATTERN_ITEM = '{item:02d}'

# # seqinfo fields
# COLS_DIMS = ['dim1', 'dim2', 'dim3'] # NOTE this assumes these are the only dimensions
# THRESHOLD_2D = 80
# THRESHOLD_BAD = 1000

# HEURISTIC_HELPER = None
# DESCRIPTIONS_NOT_FOUND = []
# BAD_DIMS = []


def run(fpath_global_config, session_id, dpath_out=None, fname_heudiconv=".heudiconv"):

    # global HEURISTIC_HELPER

    with open(fpath_global_config, "r") as file_global_config:
        global_config = json.load(file_global_config)

    dpath_dataset_root = Path(global_config["DATASET_ROOT"])

    heuristic_helper = HeuristicHelper(
        fpath_imaging=(dpath_dataset_root / "tabular" / "other" / FNAME_IMAGING),
        fpath_descriptions=(DPATH_CURRENT / ".." / "tabular" / FNAME_DESCRIPTIONS),
    )

    if dpath_out is not None:
        dpath_out = Path(dpath_out)

    dpath_heudiconv = dpath_dataset_root / "bids" / fname_heudiconv

    error_messages = []

    for dpath_subject in dpath_heudiconv.iterdir():

        subject = dpath_subject.name
        fpath_info = dpath_subject / "info" / f"dicominfo_{session_id}.tsv"

        if not fpath_info.exists():
            error_messages.append(f"No info file found for {subject} {session_id}")
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
            # raise exception

        for key, value in info.items():
            template = key[0]
            print(f'{template}:\t{",".join(value)}')

            if dpath_out is not None:

                for i in range(len(value)):
                    fpath_out: Path = (
                        dpath_out
                        / template.format(
                            subject=subject, session=session_id, item=i + 1
                        )
                    ).with_suffix(".nii.gz")

                    fpath_out.parent.mkdir(parents=True, exist_ok=True)
                    fpath_out.touch()

    print("===== ERRORS =====")
    for error_message in error_messages:
        print(error_message)

    # print('===== DESCRIPTIONS WITH ERRORS =====')
    # for description in DESCRIPTIONS_NOT_FOUND:
    #     print(description)

    # print(f'===== IMAGES WITH BAD DIMENSIONS (>= {THRESHOLD_BAD}) =====')
    # # print(','.join([str(image_id) for image_id in BAD_DIMS]))
    # print(','.join([str(image_id) for image_id, _ in BAD_DIMS]))
    # for image_id, dims in BAD_DIMS:
    #     print(f'{image_id}\t{dims}')


# def infotodict(seqinfo):

#     global HEURISTIC_HELPER
#     global DESCRIPTIONS_NOT_FOUND
#     global BAD_DIMS

#     if HEURISTIC_HELPER is None:
#         HEURISTIC_HELPER = HeuristicHelper(dpath_dataset_root='/scratch')

#     info = defaultdict(list)
#     test_info = defaultdict(list)
#     for _, s in enumerate(seqinfo):

#         image_id = get_image_id_from_dcm(s.example_dcm_file)

#         # # first check the dimension (sanity check)
#         # seqinfo_dims = [getattr(s, col_dim) for col_dim in COLS_DIMS]
#         # if any([int(dim) >= THRESHOLD_BAD for dim in seqinfo_dims]):
#         #     # BAD_DIMS.append(image_id)
#         #     BAD_DIMS.append((image_id, seqinfo_dims))
#         #     continue

#         try:
#             datatype, suffix = HEURISTIC_HELPER.get_datatype_suffix_from_description(s.series_description)
#         except TypeError:
#             # raise RuntimeError(f'Did not find description {s.series_description} in mapping for datatypes {HEURISTIC_HELPER.datatypes} for image {image_id}')
#             print(f'Did not find description {s.series_description} in mapping for datatypes {HEURISTIC_HELPER.datatypes} for image {image_id}')
#             DESCRIPTIONS_NOT_FOUND.append(s.series_description)

#         imaging_protocol_info = {}
#         for protocol_info_entry in HEURISTIC_HELPER.df_imaging.loc[image_id, COL_PROTOCOL].split(SEP_PROTOCOL_INFO_ENTRY):
#             key, value = protocol_info_entry.split(SEP_PROTOCOL_INFO)
#             imaging_protocol_info[key] = value

#         plane = None # to fill in
#         dims = None
#         if datatype == DATATYPE_ANAT:

#             if re.search(RE_NEUROMELANIN, s.series_description):
#                 info[create_key_anat(suffix, acq=TAG_NEUROMELANIN)].append(s.series_id)
#                 test_info[create_key_anat(suffix, acq=TAG_NEUROMELANIN)[0]].append(image_id)

#             else:

#                 # image dimensions: 2D or 3D
#                 try:
#                     dims = MAP_DIMS[imaging_protocol_info[KEY_DIMS]]

#                 except KeyError:
#                     # check the number of slices in seqinfo
#                     seqinfo_dims = [getattr(s, col_dim) for col_dim in COLS_DIMS]
#                     if any([int(dim) <= THRESHOLD_2D for dim in seqinfo_dims]):
#                         dims = TAG_2D
#                     else:
#                         dims = TAG_3D
#                     print(f'Using heuristic to determine number of dimensions: {seqinfo_dims} -> {dims} (image {image_id})')

#                 # acquisition plane: sagittal, coronal, or axial
#                 try:
#                     plane = MAP_PLANE[imaging_protocol_info[KEY_PLANE]]
#                 except KeyError:
#                     # TODO
#                     pass
#                     # print(f'Acquisition plane not found for image {image_id}')

#                 # if plane is None or dims is None:
#                 #     raise RuntimeError(f'Could not determine plane or dims for image {image_id} (plane: {plane}, dims: {dims})')

#                 info[create_key_anat(suffix, plane=plane, dims=dims)].append(s.series_id)
#                 test_info[create_key_anat(suffix, plane=plane, dims=dims)[0]].append(image_id)

#         # TODO dwi
#         elif datatype == DATATYPE_DWI:

#             # TODO come up with heuristic for LR/LR/AP/PA
#             acq = get_dwi_acq_from_description(s.series_description)
#             dir = get_dwi_direction_from_description(s.series_description)

#             # if dir is None:
#             #     raise RuntimeError(f'Could not determine direction for image {image_id} with description {s.series_description}')

#             info[create_key_dwi('dwi', dir=dir, acq=acq)].append(s.series_id)
#             test_info[create_key_dwi('dwi', dir=dir, acq=acq)[0]].append(image_id)

#             # # find dicom file using s.example_dcm_file
#             # phase_encoding_direction = None
#             # try:
#             #     fpath_dicom = f'/scratch/dicom/ses-*/{s.patient_id}/{s.example_dcm_file}'
#             #     phase_encoding_direction = get_bids_phase_encoding_direction(fpath_dicom)
#             # except FileNotFoundError:
#             #     try:
#             #         fpath_dicom = f'/scratch/scratch/raw_dicom/ses-*/{s.patient_id}/{s.example_dcm_file}'
#             #         phase_encoding_direction = get_bids_phase_encoding_direction(fpath_dicom)
#             #     except Exception as exception:
#             #         print(f'Error finding phase encoding directio for {fpath_dicom}: {exception}')

#             # if phase_encoding_direction is not None:
#             #     pass # TODO
#             # test_info['diffusion'].append(image_id)
#             # continue

#         else:
#             raise NotImplementedError(f'Not implemented for datatype {datatype} yet')

#     # return info
#     return test_info

# def create_key_anat(suffix, plane=None, dims=None, acq=None):

#     if (acq is not None) and (plane is not None or dims is not None):
#         raise RuntimeError('Cannot specify both acq and plane/dims')

#     if (plane is not None) and (dims is not None):
#         stem = f'sub-{{subject}}_{{session}}_acq-{plane}{dims}_run-{PATTERN_ITEM}_{suffix}'
#     elif acq is not None:
#         stem = f'sub-{{subject}}_{{session}}_acq-{acq}_run-{PATTERN_ITEM}_{suffix}'
#     else:
#         stem = f'sub-{{subject}}_{{session}}_run-{PATTERN_ITEM}_{suffix}'

#     return create_key(DATATYPE_ANAT, stem)

# def create_key_dwi(suffix, dir=None, acq=None):

#     if dir is not None:
#         dir_tag = f'_dir-{dir}'
#     else:
#         dir_tag = ''

#     if acq is not None:
#         acq_tag = f'_acq-{acq}'
#     else:
#         acq_tag = ''

#     stem = f'sub-{{subject}}_{{session}}{acq_tag}{dir_tag}_run-{PATTERN_ITEM}_{suffix}'
#     return create_key(DATATYPE_DWI, stem)

# def create_key(datatype, stem, outtype=('nii.gz',), annotation_classes=None):
#     template = f'sub-{{subject}}/{{session}}/{datatype}/{stem}'
#     return template, outtype, annotation_classes

# def get_image_id_from_dcm(fname_dcm):
#     match = re.match(RE_IMAGE_ID, fname_dcm)
#     if not match:
#         raise RuntimeError(f'Could not get image ID from {fname_dcm}')
#     if len(match.groups()) > 1:
#         raise RuntimeError(f'Got more than one image ID from {fname_dcm}')
#     return match.group(1)

# def get_dwi_acq_from_description(description: str):
#     return DESCRIPTION_ACQ_MAP.get(description)

# def get_dwi_direction_from_description(description: str):

#     for dir, descriptions in DIR_DESCRIPTIONS_MAP.items():
#         if description in descriptions:
#             return dir

#     for dir, re_dir in DIR_RE_MAP.items():
#         if re.search(re_dir, description):
#             return dir

#     return None

# # def get_bids_phase_encoding_direction(dicom_path):
# #     dcm = pydicom.read_file(dicom_path)
# #     inplane_pe_dir = dcm[int('00181312', 16)].value
# #     csa_str = dcm[int('00291010', 16)].value
# #     csa_tr = csareader.read(csa_str)
# #     pedp = csa_tr['tags']['PhaseEncodingDirectionPositive']['items'][0]
# #     ij = ROWCOL_TO_NIFTIDIM[inplane_pe_dir]
# #     sign = PEDP_TO_SIGN[pedp]
# #     return '{}{}'.format(ij, sign)

# class HeuristicHelper:

#     datatypes = [DATATYPE_ANAT, DATATYPE_DWI]
#     suffixes_anat = [SUFFIX_T1, SUFFIX_T2, SUFFIX_T2_STAR, SUFFIX_FLAIR]

#     def __init__(self, dpath_dataset_root=None, fpath_global_config=None) -> None:

#         if (dpath_dataset_root is None) and (fpath_global_config is None):
#             raise ValueError('Either fpath_dataset_root or fpath_global_config must be specified')

#         if fpath_global_config is None:
#             fpath_global_config = Path(dpath_dataset_root) / 'proc' / 'global_configs.json'

#         self.fpath_global_config = fpath_global_config

#         with open(self.fpath_global_config, 'r') as file_global_config:
#             self.global_configs = json.load(file_global_config)

#         self.dataset_root = Path(self.global_configs['DATASET_ROOT'])

#         self.fpath_imaging = self.dataset_root / 'tabular' / 'other' / FNAME_IMAGING
#         self.fpath_descriptions = Path(__file__).parent / '..' / '..' / 'tabular' / FNAME_DESCRIPTIONS

#         if not self.fpath_imaging.exists():
#             raise FileNotFoundError(f'Imaging info file {self.fpath_imaging} does not exist')

#         if not self.fpath_descriptions.exists():
#             raise FileNotFoundError(f'Descriptions map file {self.fpath_descriptions} does not exist')

#         self.df_imaging = load_and_process_df_imaging(self.fpath_imaging).set_index(COL_IMAGE_ID)

#         with open(self.fpath_descriptions, 'r') as file_descriptions:
#             self.descriptions_map = json.load(file_descriptions)

#     def get_descriptions(self, keys) -> list[str]:

#         keys_all = keys[:]

#         descriptions = self.descriptions_map
#         while len(keys) > 0:
#             key = keys.pop(0)
#             try:
#                 descriptions = descriptions[key]
#             except KeyError:
#                 raise KeyError(f'Invalid keys: {keys_all} (error at key {key})')

#         if not ((isinstance(descriptions, Sequence) and isinstance(descriptions[0], str))):
#             raise RuntimeError(f'Did not get expected format for descriptions: {descriptions} (keys: {keys_all})')

#         return descriptions

#     def get_datatype_suffix_from_description(self, description: str):
#         description = description.strip()
#         for datatype in self.datatypes:
#             if datatype == DATATYPE_ANAT:
#                 for suffix in self.suffixes_anat:
#                     if description in self.get_descriptions([datatype, suffix]):
#                         return datatype, suffix
#             else:
#                 if description in self.get_descriptions([datatype]):
#                     return datatype, None

if __name__ == "__main__":

    HELPTEXT = """
    Dummy script to test filenames in DICOM-to-BIDS conversion (HeuDiConv).
    Run bids_conv --stage 1 first, then run this, check, and run bids_conv --stage 2.
    """

    parser = argparse.ArgumentParser(description=HELPTEXT)

    parser.add_argument(
        "--global_config",
        type=str,
        required=True,
        help="path to global configs for a given mr_proc dataset",
    )
    parser.add_argument("--session_id", type=str, required=True, help="session ID")
    parser.add_argument(
        "--heudiconv", default=".heudiconv", type=str, help=".heudiconv directory name"
    )
    parser.add_argument(
        "--out",
        default="fake_bids",
        type=str,
        help="output directory for dummy BIDS files",
    )

    args = parser.parse_args()

    global_config = args.global_config
    session_id = f"ses-{args.session_id}"
    fname_heudiconv = args.heudiconv

    if args.out is not None:
        dpath_out = DPATH_CURRENT / args.out
    else:
        dpath_out = None

    run(global_config, session_id, dpath_out, fname_heudiconv=fname_heudiconv)
