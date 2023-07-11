import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Sequence

import pandas as pd # part of Heudiconv container

# BIDS standard
DATATYPE_ANAT = 'anat'
DATATYPE_DWI = 'dwi'
SUFFIX_T1 = 'T1w'
SUFFIX_T2 = 'T2w'
SUFFIX_T2_STAR = 'T2starw'
SUFFIX_FLAIR = 'FLAIR'

# datatype/suffix to description mapping (from mr_proc-ppmi script)
# this file needs to be copied to the right directory before creating the container
FPATH_DESCRIPTIONS = Path('/scratch/proc/ppmi_imaging_descriptions.json')

# LONI IDA Search result file
FPATH_IMAGING = Path('/scratch/tabular/other/idaSearch.csv') # TODO update when this file gets moved
COL_PROTOCOL = 'Imaging Protocol'
COL_IMAGE_ID = 'Image ID'
RE_IMAGE_ID = '.*_I([0-9]+).dcm' # regex
RE_NEUROMELANIN = '[nN][mM]' # neuromelanin pattern
SEP_PROTOCOL_INFO_ENTRY = ';'
SEP_PROTOCOL_INFO = '='
KEY_PLANE = 'Acquisition Plane'
KEY_DIMS = 'Acquisition Type'

# dwi directions
VALID_DIRS = ['AP', 'PA', 'LR', 'RL']
DIR_RE_MAP = {
    # will catch: ' R L', '_RL', 'R-L', 'R > L', etc.
    dir: f'[ \-_]{dir[0]}[ \-_>]*{dir[1]}(?:[ \-_]|\Z)'
    for dir in VALID_DIRS
}
# for descriptions not handled by the above regex
DIR_DESCRIPTIONS_MAP = {
    'LR': [
        '2D DTI EPI FAT SHIFT LEFT',
        'AX DTI 32 DIR FAT SHIFT L',
        'AX DTI 32 DIR FAT SHIFT L NO ANGLE',
        'AX DTI _reverse', # one subject has this and 'AX DTI _RL'

    ],
    'RL': [
        '2D DTI EPI FAT SHIFT RIGHT',
        'AX DTI 32 DIR FAT SHIFT R',
        'AX DTI 32 DIR FAT SHIFT R NO ANGLE'
    ]
}

# dwi acquisitions (for AP/PA scans)
DESCRIPTION_ACQ_MAP = {
    'DTI_B0_PA': 'B0',
    'DTI_revB0_AP': 'B0',
    'DTI_B700_64dir_PA': 'B700',
    'DTI_B1000_64dir_PA': 'B1000',
    'DTI_B2000_64dir_PA': 'B2000',
}

# acq tags for anatomical scans
TAG_NEUROMELANIN = 'NM'
TAG_SAG = 'sag'
TAG_COR = 'cor'
TAG_AX = 'ax'
TAG_2D = '2D'
TAG_3D = '3D'
MAP_PLANE = {'SAGITTAL': TAG_SAG, 'CORONAL': TAG_COR, 'AXIAL': TAG_AX}
MAP_DIMS = {'2D': TAG_2D, '3D': TAG_3D}

# format for runs
PATTERN_ITEM = '{item:02d}'

HEURISTIC_HELPER = None

def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where
    allowed template fields - follow python string module:
    item: index within category
    subject: participant id
    session: session id (including 'ses-' prefix)
    """
    
    global HEURISTIC_HELPER
    if HEURISTIC_HELPER is None:
        HEURISTIC_HELPER = HeuristicHelper()

    info = defaultdict(list)
    for _, s in enumerate(seqinfo):
        
        image_id = get_image_id_from_dcm(s.example_dcm_file)
        try:
            datatype, suffix = HEURISTIC_HELPER.get_datatype_suffix_from_description(s.series_description)
        except TypeError:
            raise RuntimeError(f'Did not find description {s.series_description} in mapping for datatypes {HEURISTIC_HELPER.datatypes} for image {image_id}')

        imaging_protocol_info = {}
        for protocol_info_entry in HEURISTIC_HELPER.df_imaging.loc[image_id, COL_PROTOCOL].split(SEP_PROTOCOL_INFO_ENTRY):
            key, value = protocol_info_entry.split(SEP_PROTOCOL_INFO)
            imaging_protocol_info[key] = value

        plane = None # to fill in
        dims = None
        if datatype == DATATYPE_ANAT:

            if re.search(RE_NEUROMELANIN, s.series_description):
                info[create_key_anat(suffix, acq=TAG_NEUROMELANIN)].append(s.series_id)

            else:

                # image dimensions: 2D or 3D
                try:
                    dims = MAP_DIMS[imaging_protocol_info[KEY_DIMS]]
                except KeyError:
                    pass

                # acquisition plane: sagittal, coronal, or axial
                try:
                    plane = MAP_PLANE[imaging_protocol_info[KEY_PLANE]]
                except KeyError:
                    pass

                info[create_key_anat(suffix, plane=plane, dims=dims)].append(s.series_id)

        elif datatype == DATATYPE_DWI:
            acq = get_dwi_acq_from_description(s.series_description)
            dir = get_dwi_dir_from_description(s.series_description)
            info[create_key_dwi('dwi', dir=dir, acq=acq)].append(s.series_id)

        else:
            raise NotImplementedError(f'Not implemented for datatype {datatype}')

    return info

def create_key_anat(suffix, plane=None, dims=None, acq=None):

    if (acq is not None) and (plane is not None or dims is not None):
        raise RuntimeError('Cannot specify both acq and plane/dims')
    
    if (plane is not None) and (dims is not None):
        stem = f'sub-{{subject}}_{{session}}_acq-{plane}{dims}_run-{PATTERN_ITEM}_{suffix}'
    elif acq is not None:
        stem = f'sub-{{subject}}_{{session}}_acq-{acq}_run-{PATTERN_ITEM}_{suffix}'
    else:
        stem = f'sub-{{subject}}_{{session}}_run-{PATTERN_ITEM}_{suffix}'

    return create_key(DATATYPE_ANAT, stem)

def create_key_dwi(suffix, dir=None, acq=None):

    if dir is not None:
        dir_tag = f'_dir-{dir}'
    else:
        dir_tag = ''

    if acq is not None:
        acq_tag = f'_acq-{acq}'
    else:
        acq_tag = ''

    stem = f'sub-{{subject}}_{{session}}{acq_tag}{dir_tag}_run-{PATTERN_ITEM}_{suffix}'
    return create_key(DATATYPE_DWI, stem)

def create_key(datatype, stem, outtype=('nii.gz',), annotation_classes=None):
    template = f'sub-{{subject}}/{{session}}/{datatype}/{stem}'
    return template, outtype, annotation_classes

def get_image_id_from_dcm(fname_dcm):
    match = re.match(RE_IMAGE_ID, fname_dcm)
    if not match:
        raise RuntimeError(f'Could not get image ID from {fname_dcm}')
    if len(match.groups()) > 1:
        raise RuntimeError(f'Got more than one image ID from {fname_dcm}')
    return match.group(1)

def get_dwi_acq_from_description(description: str):
    return DESCRIPTION_ACQ_MAP.get(description) # returns None if not found

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

    def __init__(self) -> None:

        self.fpath_imaging = FPATH_IMAGING
        self.fpath_descriptions = FPATH_DESCRIPTIONS

        if not self.fpath_imaging.exists():
            raise FileNotFoundError(f'Imaging info file {self.fpath_imaging} does not exist')
        
        if not self.fpath_descriptions.exists():
            raise FileNotFoundError(f'Descriptions map file {self.fpath_descriptions} does not exist')
        
        self.df_imaging = pd.read_csv(self.fpath_imaging, dtype=str).set_index(COL_IMAGE_ID)
        
        with open(self.fpath_descriptions, 'r') as file_descriptions:
            self.descriptions_map = json.load(file_descriptions)

    def get_descriptions(self, keys) -> list[str]:

        keys_all = keys[:]

        descriptions = self.descriptions_map
        while len(keys) > 0:
            key = keys.pop(0)
            try:
                descriptions = descriptions[key]
            except KeyError:
                raise KeyError(f'Invalid keys: {keys_all} (error at key {key})')
        
        if not ((isinstance(descriptions, Sequence) and isinstance(descriptions[0], str))):
            raise RuntimeError(f'Did not get expected format for descriptions: {descriptions} (keys: {keys_all})')
        
        return descriptions
    
    def get_datatype_suffix_from_description(self, description: str):
        description = description.strip()
        for datatype in self.datatypes:
            if datatype == DATATYPE_ANAT:
                for suffix in self.suffixes_anat:
                    if description in self.get_descriptions([datatype, suffix]):
                        return datatype, suffix
            else:
                raise NotImplementedError(f'Not implemented for datatype {datatype}')
