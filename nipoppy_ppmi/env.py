# ========== PATHS ==========
DEFAULT_FNAME_IMAGING_DESCRIPTIONS = (
    "ppmi_imaging_descriptions.json"  # output file name
)
DEFAULT_FNAME_IMAGING_IGNORED = "ppmi_imaging_ignored.csv"  # output file name

# ========== DATATYPES ==========
DATATYPE_DWI = "dwi"  # BIDS standard
DATATYPE_FUNC = "func"
DATATYPE_ANAT = "anat"
SUFFIX_T1 = "T1w"  # BIDS standard (file suffix)
SUFFIX_T2 = "T2w"
SUFFIX_T2_STAR = "T2starw"
SUFFIX_FLAIR = "FLAIR"

# ========== TABULAR CSV COLUMNS ==========
COL_SUBJECT_TABULAR = "PATNO"
COL_VISIT_TABULAR = "EVENT_ID"
COL_GROUP_TABULAR = "COHORT_DEFINITION"

# ========== IMAGING CSV COLUMNS ==========
COL_SUBJECT_IMAGING = "Subject ID"
COL_SESSION_IMAGING = "Visit"
COL_GROUP_IMAGING = "Research Group"
COL_MODALITY_IMAGING = "Modality"  # column name in PPMI schema
COL_DESCRIPTION_IMAGING = "Description"
COL_PROTOCOL_IMAGING = "Imaging Protocol"

MODALITY_DWI = "DTI"  # PPMI "Modality" column
MODALITY_FUNC = "fMRI"
MODALITY_ANAT = "MRI"

# ========== MAPPINGS ==========
VISIT_IMAGING_MAP = {
    "Baseline": "BL",
    "Month 6": "R01",
    "Month 12": "V04",
    "Month 24": "V06",
    "Month 36": "V08",
    "Month 48": "V10",
    "Screening": "SC",
    "Premature Withdrawal": "PW",
    "Symptomatic Therapy": "ST",
    "Unscheduled Visit 01": "U01",
    "Unscheduled Visit 02": "U02",
}
GROUP_IMAGING_MAP = {
    "PD": "Parkinson's Disease",
    "Prodromal": "Prodromal",
    "Control": "Healthy Control",
    "Phantom": "Phantom",  # not in participant status file
    "SWEDD": "SWEDD",
    "GenReg Unaff": "GenReg Unaff",  # not in participant status file
}
