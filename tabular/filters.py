# ========== DATATYPES ==========
DATATYPE_DWI = 'dwi'        # BIDS standard
DATATYPE_FUNC = 'func'
DATATYPE_ANAT = 'anat'
DATATYPE_T1 = 't1'          # not actually BIDS but useful for HeuDiConv
DATATYPE_T2 = 't2'
DATATYPE_FLAIR = 'flair'

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
