# ========== DATATYPES ==========
DATATYPE_DWI = 'dwi'        # BIDS standard
DATATYPE_FUNC = 'func'
DATATYPE_ANAT = 'anat'
SUFFIX_T1 = 'T1w'           # BIDS standard (file suffix)
SUFFIX_T2 = 'T2w'
SUFFIX_T2_STAR = 'T2starw'
SUFFIX_FLAIR = 'FLAIR'

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
            'DTI_FA',       # phantom (solar eclipse)
        ],
        'exclude_out': [
            'PPMI 2.0',
        ],
        'reject_substrings': ['phantom'],
    },
    DATATYPE_FUNC: {
        'common_substrings': ['fmri', 'bold', 'rsmri'],
        'exclude_in': [
            'NM - MT',      # neuromelanin
            '2 NM-GRE',     # neuromelanin
            '2D GRE_MT',    # 2D
            '2D GRE-MT',    # 2D
        ],
        'reject_substrings': ['phantom'],
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
COMMON_SUBSTRINGS_ANAT_T2 = ['t2', 'nm'] # including neuromelanin in T2
COMMON_SUBSTRINGS_ANAT_T2_STAR = ['t2_star', 't2\*']
COMMON_SUBSTRINGS_ANAT_FLAIR = ['flair']
EXCLUDE_IN_ANAT = [
    # 2D
    'ax t1 reformat',
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
    'TRA',          # 55 slices in one dimension
    'SAG',          # 55 slices in one dimension
    'COR',          # 55 slices in one dimension
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
    "t2_tirm_tra_dark-fluid NO BLADE",  # weird FLAIR
    "t2_tirm_tra_dark-fluid_",          # weird FLAIR
    # clipped
    'Transverse',   # top/bottom of brain not complete
    'Coronal',      # front/back of brain not complete
]
EXCLUDE_IN_ANAT_T1 = EXCLUDE_IN_ANAT + ['Ax 3D SWAN GRE straight', 'MRI BRAIN WO IVCON']
REJECT_SUBSTRINGS_ANAT = ['2d', 'phantom'] + FILTERS[DATATYPE_DWI]['common_substrings'] + FILTERS[DATATYPE_FUNC]['common_substrings']
FILTERS.update({
    SUFFIX_T1: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_T1,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T2 + COMMON_SUBSTRINGS_ANAT_T2_STAR + COMMON_SUBSTRINGS_ANAT_FLAIR,
        'reject_substrings_exceptions': ['T1 REPEAT2'], # contains 'T2'
    },
    SUFFIX_T2: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_T2,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T1 + COMMON_SUBSTRINGS_ANAT_T2_STAR + COMMON_SUBSTRINGS_ANAT_FLAIR,
    },
    SUFFIX_T2_STAR: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_T2_STAR,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T1 + COMMON_SUBSTRINGS_ANAT_FLAIR,
    },
    SUFFIX_FLAIR: {
        'common_substrings': COMMON_SUBSTRINGS_ANAT_FLAIR,
        'reject_substrings': REJECT_SUBSTRINGS_ANAT + COMMON_SUBSTRINGS_ANAT_T1 + COMMON_SUBSTRINGS_ANAT_T2_STAR,
    }
})
