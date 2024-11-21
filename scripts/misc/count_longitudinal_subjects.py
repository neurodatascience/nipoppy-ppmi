#!/usr/bin/env python
from pathlib import Path

import pandas as pd

COL_SUBJECT = "participant_id"
COL_SESSION = "session"
COL_DATATYPE = "datatype"
DATATYPES_TO_CHECK = ["neuromelanin"]
# DATATYPES_TO_CHECK = ["anat", "dwi", "neuromelanin"] # can also have "neuromelanin"
# DATATYPES_TO_CHECK = ['dwi']
SESSION_BL = "ses-BL"

COL_SUBJECT_PPMI = "PATNO"
COL_STATUS_PPMI = "COHORT_DEFINITION"
STATUSES_TO_CHECK = [
    # "Healthy Control",
    # "Parkinson's Disease",
    'Prodromal',
]  # 'Parkinson\'s Disease', 'Prodromal', 'SWEDD', 'Healthy Control'

COL_HAS_IMAGING = "has_imaging"

fpath_current = Path(__file__).parent
fpath_manifest = fpath_current / ".." / "manifest.csv"
fpath_cohort = fpath_current / ".." / "demographics" / "Participant_Status.csv"

df_manifest = pd.read_csv(fpath_manifest)
print(df_manifest)

df_cohort = pd.read_csv(fpath_cohort)
print(df_cohort)

df_manifest = df_manifest.merge(
    df_cohort.set_index(COL_SUBJECT_PPMI)[COL_STATUS_PPMI],
    left_on=COL_SUBJECT,
    right_index=True,
)
print(df_manifest)

for datatype in DATATYPES_TO_CHECK:
    if datatype == 'neuromelanin':
        continue
    df_manifest[datatype] = df_manifest[COL_DATATYPE].apply(lambda x: datatype in x)
df_manifest[COL_HAS_IMAGING] = df_manifest[DATATYPES_TO_CHECK].all(axis="columns")
print(df_manifest)

df_manifest_subset = df_manifest.loc[
    (df_manifest[COL_HAS_IMAGING])
    & (df_manifest[COL_STATUS_PPMI].isin(STATUSES_TO_CHECK))
]
print(df_manifest_subset)

session_counts = df_manifest_subset.groupby(COL_SUBJECT).size()
print(session_counts)

subjects_with_two_or_more_sessions = session_counts[session_counts >= 2].index
print(
    f"Found {len(set(df_manifest_subset[COL_SUBJECT]))} subjects ({STATUSES_TO_CHECK}) with imaging data ({DATATYPES_TO_CHECK})"
)
print(
    f"Found {len(subjects_with_two_or_more_sessions)} subjects ({STATUSES_TO_CHECK}) with two or more imaging sessions"
)

df_manifest_subset_with_baseline = df_manifest_subset.loc[
    (df_manifest_subset[COL_SESSION] == SESSION_BL)
    & (df_manifest_subset[COL_SUBJECT].isin(subjects_with_two_or_more_sessions))
]
print(
    f"Found {len(subjects_with_two_or_more_sessions)} subjects ({STATUSES_TO_CHECK}) with two or more imaging sessions (including baseline)"
)
