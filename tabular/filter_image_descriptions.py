#!/usr/bin/env python

import argparse
import json
from pathlib import Path

import pandas as pd

# ========== CONSTANTS ==========
DEFAULT_IMAGING_FILENAME = 'idaSearch.csv'
FNAME_DESCRIPTIONS = 'ppmi_imaging_descriptions.json' # output file name
FNAME_IGNORED = 'ppmi_imaging_ignored.csv'            # output file name
GLOBAL_CONFIG_DATASET_ROOT = 'DATASET_ROOT'
DPATH_INPUT_RELATIVE = Path('tabular', 'study_data')  # relative to DATASET_ROOT
FLAG_OVERWRITE = '--overwrite'

# imaging table columns
COL_MODALITY = 'Modality'
COL_DESCRIPTION = 'Description'
COL_PROTOCOL = 'Imaging Protocol'
MODALITY_DWI = 'DTI'        # PPMI Modality column
MODALITY_FUNC = 'fMRI'
MODALITY_ANAT = 'MRI'

# datatype names and mapping to modalities
DATATYPE_DWI = 'dwi'        # BIDS standard
DATATYPE_FUNC = 'func'
DATATYPE_ANAT = 'anat'
DATATYPE_T1 = 't1'          # not actually BIDS but useful for HeuDiConv
DATATYPE_T2 = 't2'
DATATYPE_FLAIR = 'flair'
DATATYPE_MODALITY_MAP = {
    DATATYPE_DWI: MODALITY_DWI,
    DATATYPE_FUNC: MODALITY_FUNC,
    DATATYPE_ANAT: MODALITY_ANAT,
    DATATYPE_T1: MODALITY_ANAT,        # anat
    DATATYPE_T2: MODALITY_ANAT,        # anat
    DATATYPE_FLAIR: MODALITY_ANAT,     # anat
}

# ========== FILTERS ==========
# Heuristics for assigning a datatype based on image description
# 'common_substrings'   substrings commonly found in descriptions strings for this datatype
# 'exclude_in'          within-modality exclude list
# 'exclude_out'         out-of-modality exclude list
# 'reject_substrings'   drop all descriptions with these substrings (within and out of modality)
# ----- DWI + FUNC + ANAT (partial) -----
FILTERS = {
    DATATYPE_DWI: {
        'common_substrings': ['dti', 'dw', 'DT_SSh_iso'],
        'exclude_in': [
            'T1', 
            'T2', 
            'sT1W_3D_TFE', 
            'TRA/DUAL',     # SWI/FLAIR
            'MR',           # phantom subject
        ],
        'exclude_out': [
            'PPMI 2.0',
        ],
    },
    DATATYPE_FUNC: {
        'common_substrings': ['fmri', 'bold', 'rsmri'],
        'exclude_in': [
            'NM - MT',      # neuromelanin
            '2 NM-GRE',     # neuromelanin
            '2D GRE_MT',    # 2D
            '2D GRE-MT',    # 2D
        ],
    },
    DATATYPE_ANAT: {
        'exclude_out': [
            'PPMI 2.0',
            'TRA/DUAL',
        ],
    }
}
# ----- ANAT (T1/T2/FLAIR) -----
COMMON_SUBSTRINGS_ANAT_T1 = ['t1', 'mprage']
COMMON_SUBSTRINGS_ANAT_T2 = ['t2']
COMMON_SUBSTRINGS_ANAT_FLAIR = ['flair']
EXCLUDE_IN_ANAT = [
    # neuromelanin
    'NM - MT',
    '2 NM-GRE',
    'NM-MT',
    'NM-GRE',
    # 2D
    'AX GRE -MT',
    'AX DUAL_TSE',
    'DUAL_TSE',
    'TRA/DUAL',
    'AX DE TSE',
    'SURVEY',
    'Double_TSE',
    'localizer',
    'AX GRE -MT REPEAT',
    '3 Plane Localizer',
    # 'TRA',          # 55 slices in one dimension
    # 'SAG',          # 55 slices in one dimension
    # 'COR',          # 55 slices in one dimension
    'LOCALIZER',
    '3 plane',
    '3 PLANE LOC',
    'HighResHippo',
    'MIDLINE SAG LOC',
    'AX PD  5/1',
    'sag',
    # other
    'B0rf Map',
    'Cal Head 24',
    'SAG SPGR',     # field strength 0.7 Tesla
    'Anon',         # not anat
    'Field_mapping',
    'GRE B0',
    'IsoADC',       # not anat
    # clipped
    'Transverse',   # top/bottom of brain not complete
    'Coronal',      # front/back of brain not complete
    'SAG FSPGR 3D', # most/all are clipped and the contrast seems unusual (?)
]
EXCLUDE_IN_ANAT_T1 = EXCLUDE_IN_ANAT + ['Ax 3D SWAN GRE straight']
REJECT_SUBSTRINGS_ANAT = ['2d'] + FILTERS[DATATYPE_DWI]['common_substrings'] + FILTERS[DATATYPE_FUNC]['common_substrings']
FILTERS.update({
    DATATYPE_T1: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_T1,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T2 + COMMON_SUBSTRINGS_ANAT_FLAIR,
        'reject_substrings_exceptions': ['T1 REPEAT2'], # contains 'T2'
    },
    DATATYPE_T2: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_T2,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T1 + COMMON_SUBSTRINGS_ANAT_FLAIR,
    },
    DATATYPE_FLAIR: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_FLAIR,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T1,
    }
})


def filter_descriptions(
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
    protocol_filters_str = ''
    if protocol_include is not None:
        df = df.loc[
            df[COL_PROTOCOL].str.lower().str.contains('|'.join(protocol_include).lower(), na=False)
        ]
        protocol_filters_str = f'{protocol_filters_str}\n\t- WITH: {", ".join(protocol_include)}'
    if protocol_exclude is not None:
        df = df.loc[
            ~df[COL_PROTOCOL].str.lower().str.contains('|'.join(protocol_exclude).lower(), na=False)
        ]
        protocol_filters_str = f'{protocol_filters_str}\n\t- WITHOUT: {", ".join(protocol_exclude)}'
    
    # initial set of descriptions
    df_modality = df.loc[df[COL_MODALITY] == modality]
    descriptions = df_modality[COL_DESCRIPTION].value_counts()
    print(f'Found {len(descriptions)} unique description strings for modality {modality}{protocol_filters_str}')

    # remove bad descriptions
    if exclude_in is not None and len(exclude_in) > 0:
        descriptions = descriptions.loc[~descriptions.index.isin(exclude_in)]
        if len(exclude_in) > 10:
            exclude_in_str = f'{len(exclude_in)} descriptions'
        else:
            exclude_in_str = f'{exclude_in}'
        print(f'\nGot {len(descriptions)} unique descriptions after removing {exclude_in_str}')

    # filter based on description strings (substring matching)
    if reject_substrings is not None and len(reject_substrings) > 0:

        if reject_substrings_exceptions is not None:
            descriptions_keep = descriptions.loc[
                descriptions.index.str.lower().str.contains('|'.join(reject_substrings_exceptions).lower())
            ]
            reject_substrings_exceptions_str = f' (except {reject_substrings_exceptions})'
        else:
            descriptions_keep = None
            reject_substrings_exceptions_str = ''

        descriptions = descriptions.loc[
            ~descriptions.index.str.lower().str.contains('|'.join(reject_substrings).lower())
        ]

        descriptions = pd.concat([descriptions, descriptions_keep])
        descriptions = descriptions.loc[~descriptions.index.duplicated()]

        print(f'\nGot {len(descriptions)} unique descriptions after removing those that contained one of {reject_substrings}{reject_substrings_exceptions_str}')

    # find descriptions that don't contain a common substring
    suspicious_descriptions = descriptions.loc[~descriptions.index.str.lower().str.contains('|'.join(common_substrings).lower())]
    print(f'\n{len(suspicious_descriptions)} descriptions out of {len(descriptions)} do not contain any of {common_substrings}')
    print(f'Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_in list')
    print('-'*30)
    print(suspicious_descriptions)

    # check other modalities
    df_other_modalities = df.loc[~df.index.isin(df_modality.index)]

    # remove known bad descriptions
    exclude_out_str = ''
    if exclude_out is not None and len(exclude_out) > 0:
        df_other_modalities = df_other_modalities.loc[
            ~df_other_modalities[COL_DESCRIPTION].isin(exclude_out)
        ]
        exclude_out_str = f' (after removing {exclude_out})'

    # check if any of the previously found descriptions are in another modality
    common_descriptions_other_modalities = df_other_modalities.loc[
        (df_other_modalities[COL_DESCRIPTION].isin(descriptions.index)),
        COL_DESCRIPTION,
    ].value_counts()
    if len(common_descriptions_other_modalities) > 0:
        print(
            f'\nFound {len(common_descriptions_other_modalities)}'
            ' common descriptions with other modalities'
        )
        print(f'Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_out list')
        print('-'*30)
        print(common_descriptions_other_modalities)

    # find descriptions in other modalities that have a common substring
    descriptions_new = df_other_modalities.loc[
        (
            (df_other_modalities[COL_DESCRIPTION].str.lower().str.contains('|'.join(common_substrings).lower()))
            & (~df_other_modalities[COL_DESCRIPTION].isin(descriptions.index))
        ),
        COL_DESCRIPTION,
    ].value_counts()
    if len(descriptions_new) > 0:
        print(
            f"\n{'!'*30}"
            f'\nFound {len(descriptions_new)} new description string(s) with one'
            f' of {common_substrings} in another modality{exclude_out_str}'
        )
        print(f'Make sure that they are indeed {datatype.upper()}, otherwise add them to exclude_out list')
        print(descriptions_new)

    # combine
    descriptions = sorted(pd.concat([descriptions, descriptions_new]).index.drop_duplicates().to_list())

    return descriptions


def run(global_config_file, imaging_filename, overwrite=False, indent=4):

    # parse global config
    with open(global_config_file) as file:
        global_config = json.load(file)
    dpath_dataset = Path(global_config[GLOBAL_CONFIG_DATASET_ROOT])

    # generate filepaths
    dpath_input = dpath_dataset / DPATH_INPUT_RELATIVE
    dpath_out = Path(__file__).parent
    fpath_imaging = dpath_input / imaging_filename
    fpath_out_descriptions = dpath_out / FNAME_DESCRIPTIONS
    fpath_out_ignored = dpath_out / FNAME_IGNORED

    # load df
    df_imaging = pd.read_csv(fpath_imaging)
    descriptions = {}

    # dwi
    print(f'========== {DATATYPE_DWI} =========='.upper())
    descriptions[DATATYPE_DWI] = filter_descriptions(
        df=df_imaging,
        datatype=DATATYPE_DWI,
        **FILTERS[DATATYPE_DWI],
    )

    # func
    print(f'\n========== {DATATYPE_FUNC} =========='.upper())
    descriptions[DATATYPE_FUNC] = filter_descriptions(
        df=df_imaging,
        datatype=DATATYPE_FUNC,
        **FILTERS[DATATYPE_FUNC],
    )

    # t1
    print(f'\n========== {DATATYPE_ANAT} ({DATATYPE_T1}) =========='.upper())
    descriptions[DATATYPE_T1] = filter_descriptions(
        df=df_imaging,
        datatype=DATATYPE_T1,
        exclude_in=EXCLUDE_IN_ANAT_T1,
        **FILTERS[DATATYPE_ANAT],
        **FILTERS[DATATYPE_T1],
    )

    # t2
    print(f'\n========== {DATATYPE_ANAT} ({DATATYPE_T2}) =========='.upper())
    descriptions[DATATYPE_T2] = filter_descriptions(
        df=df_imaging,
        datatype=DATATYPE_T2,
        exclude_in=EXCLUDE_IN_ANAT + descriptions[DATATYPE_T1],
        **FILTERS[DATATYPE_ANAT],
        **FILTERS[DATATYPE_T2],
    )

    # flair
    print(f'\n========== {DATATYPE_ANAT} ({DATATYPE_FLAIR}) =========='.upper())
    descriptions[DATATYPE_FLAIR] = filter_descriptions(
        df=df_imaging,
        datatype=DATATYPE_FLAIR,
        exclude_in=EXCLUDE_IN_ANAT + descriptions[DATATYPE_T1] + descriptions[DATATYPE_T2],
        **FILTERS[DATATYPE_ANAT],
        **FILTERS[DATATYPE_FLAIR],
    )

    # anat: T1 + T2 + FLAIR
    descriptions[DATATYPE_ANAT] = sorted(list(set(
        descriptions[DATATYPE_T1] + descriptions[DATATYPE_T2] + descriptions[DATATYPE_FLAIR]
    )))

    print(f'\nFINAL DESCRIPTIONS:')
    descriptions_all = []
    for datatype, datatype_descriptions in descriptions.items():
        descriptions_all.extend(datatype_descriptions)
        print(f'{datatype}:\t{len(datatype_descriptions)}')

    # check if output file already exists
    for fpath in [fpath_out_descriptions, fpath_out_ignored]:
        if fpath.exists() and not overwrite:
            raise FileExistsError(f'File exists: {fpath}. Use {FLAG_OVERWRITE} to overwrite')

    # save
    with fpath_out_descriptions.open('w') as file_out:
        json.dump(descriptions, file_out, indent=indent)
    print(f'JSON file with datatype-descriptions mapping written to: {fpath_out_descriptions}')

    # save another file with all images that didn't make it in any datatype
    df_ignored: pd.DataFrame = df_imaging.loc[
        ~df_imaging[COL_DESCRIPTION].isin(descriptions_all),
        COL_DESCRIPTION,
    ].drop_duplicates().sort_values()
    df_ignored.to_csv(fpath_out_ignored, index=False)
    print(f'Ignored descriptions written to: {fpath_out_ignored}')

if __name__ == '__main__':
    # argparse
    HELPTEXT = f"""
    Script to generate generate lists of protocol names for various datatypes and sub-datatypes. 
    Also creates a CSV file with all images with a description not matching any datatype. 
    Requires an imaging data availability info file that can be downloaded from 
    the LONI IDA. File should be in [DATASET_ROOT]/{DPATH_INPUT_RELATIVE}.
    """
    parser = argparse.ArgumentParser(description=HELPTEXT)
    parser.add_argument(
        '--global_config', type=str, required=True,
        help='path to global config file for your mr_proc dataset (required)')
    parser.add_argument(
        '--imaging_filename', type=str, default=DEFAULT_IMAGING_FILENAME,
        help=('name of file containing imaging data availability info, with columns'
              f' "{COL_MODALITY}", "{COL_DESCRIPTION}", and "{COL_PROTOCOL}"'
              f' (default: {DEFAULT_IMAGING_FILENAME})'))
    parser.add_argument(
        FLAG_OVERWRITE, action='store_true',
        help=(f'overwrite any existing {FNAME_DESCRIPTIONS} file')
    )
    args = parser.parse_args()

    # parse
    global_config_file = args.global_config
    imaging_filename = args.imaging_filename
    overwrite = args.overwrite

    run(global_config_file, imaging_filename, overwrite=overwrite)

