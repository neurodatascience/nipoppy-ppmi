from nipoppy_ppmi.env import (
    DATATYPE_ANAT,
    DATATYPE_DWI,
    DATATYPE_FUNC,
    SUFFIX_FLAIR,
    SUFFIX_T1,
    SUFFIX_T2,
    SUFFIX_T2_STAR,
)

# ========== FILTERS ==========
# Heuristics for assigning a datatype based on image description
# 'common_substrings'   substrings commonly found in descriptions strings for this datatype
# 'exclude_in'          within-modality exclude list
# 'exclude_out'         out-of-modality exclude list
# 'reject_substrings'   drop all descriptions with these substrings (within and out of modality)
# ----- DWI + FUNC + ANAT (partial) -----
FILTERS = {
    DATATYPE_DWI: {
        "common_substrings": ["dti", "dw", "DT_SSh_iso"],
        "exclude_in": [
            "T1",
            "T2",
            "sT1W_3D_TFE",
            "TRA/DUAL",  # SWI/FLAIR
            "MR",  # phantom subject
            "DTI_FA",  # phantom (solar eclipse)
            "DTI_gated_FA",  # not raw DWI
            "DTI Sequence_FA",  # not raw DWI
            "DTI_gated AC-PC LINE_FA",  # not raw DWI
            "DTI_LR_ColFA",  # not raw DWI
            "DTI_RL_ColFA",  # not raw DWI
            "DTI_LR_FA",  # not raw DWI
            "DTI_RL_FA",  # not raw DWI
        ],
        "exclude_out": [
            "PPMI 2.0",
            "DTI (30Axis)",
            "eDW_SSh SENSE",
            "dDW_SSh SENSE",
            "DW_SSh separate",
            "dDW_SSh ADC",
            "DTI_RL_TRACEW",
            "DTI_LR_TRACEW",
            "DTI_RL_ADC",
            "DTI_RL_FA",
            "DTI_LR_ADC",
            "DTI_LR_FA",
            "DTI_RL_ColFA",
            "DTI_LR_ColFA",
            "DTI_B1000_64dir_PA_ADC",
            "DTI_B1000_64dir_PA_TRACEW",
            "DTI_B700_64dir_PA_ADC",
            "DTI_B700_64dir_PA_TRACEW",
        ],
        "reject_substrings": ["phantom", "adc", "trace"],
    },
    DATATYPE_FUNC: {
        "common_substrings": ["fmri", "bold", "rsmri"],
        "exclude_in": [
            "NM - MT",  # neuromelanin
            "2 NM-GRE",  # neuromelanin
            "NM-MT",  # neuromelanin
            "MODIFIED 2D GRE MT MTC-NO 2 DYN COND IMPLANT", # neuromelanin
            "2D GRE_MT",  # 2D
            "2D GRE-MT",  # 2D
            "2D GRE MT MTC-NO",  # 2D
            "DTI_B0_PA",  # DTI
            "DTI_revB0_AP",  # DTI
            "t2_localizer",  # localizer
        ],
        "exclude_out": [
            "rsfMRI_PA_Do Not Use",
        ],
        "reject_substrings": ["phantom"],
    },
    DATATYPE_ANAT: {
        "exclude_out": [
            "PPMI 2.0",
            "TRA/DUAL",
            "t2_localizer",
        ],
    },
}
# ----- ANAT (T1/T2/FLAIR) -----
COMMON_SUBSTRINGS_ANAT_T1 = ["t1", "mprage", "nm"]  # neuromelanins are all T1
COMMON_SUBSTRINGS_ANAT_T2 = ["t2"]
COMMON_SUBSTRINGS_ANAT_T2_STAR = ["t2_star", "t2\*"]
COMMON_SUBSTRINGS_ANAT_FLAIR = ["flair"]
EXCLUDE_IN_ANAT = [
    # 2D
    "ax t1 reformat",
    "AX DUAL_TSE",
    "DUAL_TSE",
    "TRA/DUAL",
    "AX DE TSE",
    "SURVEY",
    "Double_TSE",
    "localizer",
    "3 Plane Localizer",
    "TRA",  # 55 slices in one dimension
    "SAG",  # 55 slices in one dimension
    "COR",  # 55 slices in one dimension
    "LOCALIZER",
    "COR T2 loc",
    "3 plane",
    "3 PLANE LOC",
    "HighResHippo",
    "MIDLINE SAG LOC",
    "AX PD  5/1",
    "sag",
    "MPR - SmartBrain",  # only 1 slice
    # other
    "B0map_v1",
    "B0rf Map",
    "Cal Head 24",
    "SAG SPGR",  # field strength 0.7 Tesla
    "Anon",  # not anat
    "Field_mapping",
    "GRE B0",
    "GRE B0 map",
    "GRE",
    "IsoADC",  # not anat
    "t2_tirm_tra_dark-fluid NO BLADE",  # weird FLAIR
    "t2_tirm_tra_dark-fluid_",  # weird FLAIR
    "MoCoSeries",
    # clipped
    "Transverse",  # top/bottom of brain not complete
    "Coronal",  # front/back of brain not complete
]
EXCLUDE_IN_ANAT_T1 = EXCLUDE_IN_ANAT + ["Ax 3D SWAN GRE straight", "MRI BRAIN WO IVCON"]
REJECT_SUBSTRINGS_ANAT = (
    ["2d", "phantom"]
    + FILTERS[DATATYPE_DWI]["common_substrings"]
    + FILTERS[DATATYPE_FUNC]["common_substrings"]
)
FILTERS.update(
    {
        SUFFIX_T1: {
            "common_substrings": COMMON_SUBSTRINGS_ANAT_T1,
            "reject_substrings": REJECT_SUBSTRINGS_ANAT
            + COMMON_SUBSTRINGS_ANAT_T2
            + COMMON_SUBSTRINGS_ANAT_T2_STAR
            + COMMON_SUBSTRINGS_ANAT_FLAIR,
            "reject_substrings_exceptions": [
                "T1 REPEAT2",  # contains 'T2'
                # neuromelanin, contains '2D'
                "2D GRE-NM",
                "2D GRE-NMMT",
                "2D GRE-NM_MT",
                "2D GRE - MT",
                "2D GRE MT",
                "2D GRE MT MTC-NO",
                "2D GRE-MT",
                "2D GRE-MT 1",
                "2D GRE-MT 2",
                "2D GRE-MT 3",
                "2D GRE-MT 4",
                "2D GRE-MT 5",
                "2D GRE-MT Q9R1007332",
                "2D GRE-MT_ACPC",
                "2D GRE-MT_RPT2",
                "2D GRE_MT",
                "2D-GRE MT",
                "2D-GRE-MT",
                "2DGRE-MT",
                "2D_GRE-MT",
                "2D_GRE_MT",
                "AX 2D GRE-MT",
                "AXIAL 2D GRE-MT",
                "LOWER 2D GRE MT",
            ],
        },
        SUFFIX_T2: {
            "common_substrings": COMMON_SUBSTRINGS_ANAT_T2,
            "reject_substrings": REJECT_SUBSTRINGS_ANAT
            + COMMON_SUBSTRINGS_ANAT_T1
            + COMMON_SUBSTRINGS_ANAT_T2_STAR
            + COMMON_SUBSTRINGS_ANAT_FLAIR,
        },
        SUFFIX_T2_STAR: {
            "common_substrings": COMMON_SUBSTRINGS_ANAT_T2_STAR,
            "reject_substrings": REJECT_SUBSTRINGS_ANAT
            + COMMON_SUBSTRINGS_ANAT_T1
            + COMMON_SUBSTRINGS_ANAT_FLAIR,
        },
        SUFFIX_FLAIR: {
            "common_substrings": COMMON_SUBSTRINGS_ANAT_FLAIR,
            "reject_substrings": REJECT_SUBSTRINGS_ANAT
            + COMMON_SUBSTRINGS_ANAT_T1
            + COMMON_SUBSTRINGS_ANAT_T2_STAR,
        },
    }
)
