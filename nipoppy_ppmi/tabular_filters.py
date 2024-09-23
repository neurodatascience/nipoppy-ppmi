import pandas as pd

from nipoppy_ppmi.env import COL_SUBJECT_TABULAR, COL_VISIT_TABULAR

COL_UPDRS3 = "NP3TOT"
COL_AGE = "AGE_AT_VISIT"
COL_EDUCATION = "EDUCYRS"
COL_UPSIT = "UPSIT_PRCNTGE"


def loading_func(df: pd.DataFrame, logger=None):
    def log(msg):
        if logger is not None:
            logger.info(msg)

    if COL_UPDRS3 in df.columns:
        log(f"Filtering {COL_UPDRS3}")
        df = updrs3_on_off_splitter(df)
    if COL_AGE in df.columns:
        log(f"Filtering {COL_AGE}")
        df = age_filter(df)
    if COL_EDUCATION in df.columns:
        log(f"Filtering {COL_EDUCATION}")
        df = education_filter(df)
    if COL_UPSIT in df.columns:
        log(f"Filtering {COL_UPSIT}")
        df = upsit_filter(df)
    return df


def updrs3_on_off_splitter(df: pd.DataFrame):

    COL_OFF = f"{COL_UPDRS3}_OFF"
    COL_ON = f"{COL_UPDRS3}_ON"
    data_new_df = []
    cols = [
        COL_SUBJECT_TABULAR,
        COL_VISIT_TABULAR,
        "PDSTATE",
        "PAG_NAME",
        "PDTRTMNT",
        COL_UPDRS3,
    ]
    for subject, session, pd_state, page, pd_treatment, updrs3 in df[cols].itertuples(
        index=False
    ):

        target_col = COL_OFF
        if pd_state == "ON":
            target_col = COL_ON
        elif pd_treatment != "0" and pd_state != "OFF":
            if page == "NUPDR3ON":
                target_col = COL_ON
            elif page != "NUPDR3OF":
                if pd_treatment == "1":
                    target_col = COL_ON

        data_new_df.append(
            {
                COL_SUBJECT_TABULAR: subject,
                COL_VISIT_TABULAR: session,
                target_col: float(updrs3),
            }
        )

    df_on_off = (
        pd.DataFrame(data_new_df)
        .groupby([COL_SUBJECT_TABULAR, COL_VISIT_TABULAR])
        .max()
        .reset_index()
    )
    return df_on_off


def _find_duplicates(df: pd.DataFrame, cols_index, col_value):
    groups = df.groupby(cols_index)[col_value]
    counts = groups.count()
    records_with_multiple_ages = counts[counts > 1].index.unique()
    df_no_duplicates = df.set_index(cols_index).drop(index=records_with_multiple_ages)
    return records_with_multiple_ages, groups, df_no_duplicates


def _subject_sort_key(series: pd.Series):
    try:
        return series.astype(int)
    except Exception:
        return series


def age_filter(df: pd.DataFrame):
    def visit_sort_key(visit):
        # custom sorting key so that the order is: SC, then BL, then the rest in numerical order
        if visit == "SC":
            return -10
        elif visit == "BL":
            return -5
        else:
            return int("".join([c for c in visit if c.isdigit()]))

    def visit_is_before_or_same(visit1, visit2):
        return visit_sort_key(visit1) <= visit_sort_key(visit2)

    df[COL_AGE] = df[COL_AGE].astype(float)

    # find subjects with multiple age entries for the same visit
    records_with_multiple_ages, groups, df_no_duplicates = _find_duplicates(
        df, [COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], COL_AGE
    )
    for record_to_fix in records_with_multiple_ages:
        # reduce duplicate ages into a single age by dropping "bad" ages and
        # taking the mean of the ages not marked as "bad"
        # "bad" ages are those that are greater than (non-duplicated) ages at later visits
        subject, session = record_to_fix
        duplicate_ages = groups.get_group(record_to_fix)
        other_sessions: pd.DataFrame = df.loc[
            (df[COL_SUBJECT_TABULAR] == subject) & (df[COL_VISIT_TABULAR] != session)
        ]
        bad_ages = []
        for duplicate_age in duplicate_ages:
            for _session, _age in other_sessions[
                [COL_VISIT_TABULAR, COL_AGE]
            ].itertuples(index=False):
                if visit_is_before_or_same(session, _session) and duplicate_age >= _age:
                    bad_ages.append(duplicate_age)
        final_age = duplicate_ages[~duplicate_ages.isin(bad_ages)].mean()

        df_no_duplicates.loc[record_to_fix, COL_AGE] = final_age

    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(
        by=[COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], key=_subject_sort_key
    )
    return df_no_duplicates


def education_filter(df: pd.DataFrame):
    # education is a "static metric", meaning that only one value is expected for
    # each participant. However, there are participants with more than one entry
    # in that case we take the mean of the entries
    df[COL_EDUCATION] = df[COL_EDUCATION].astype(float)

    # first we drop the duplicates where the education year is the same
    df = df.drop_duplicates([COL_SUBJECT_TABULAR, COL_EDUCATION])

    # we also drop rows with missing values
    df = df.dropna(axis="index", subset=COL_EDUCATION)

    subjects_with_multiple_edu, groups, df_no_duplicates = _find_duplicates(
        df, [COL_SUBJECT_TABULAR], COL_EDUCATION
    )
    for subject_to_fix in subjects_with_multiple_edu:
        duplicate_edus = groups.get_group((subject_to_fix,))
        df_no_duplicates.loc[subject_to_fix, COL_EDUCATION] = duplicate_edus.mean()
    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(
        by=COL_SUBJECT_TABULAR, key=_subject_sort_key
    )

    return df_no_duplicates


def upsit_filter(df: pd.DataFrame):
    # take the mean UPSIT score
    df[COL_UPSIT] = df[COL_UPSIT].astype(float)

    # we also drop rows with missing values
    df = df.dropna(axis="index", subset=COL_UPSIT)

    subjects_with_multiple_edu, groups, df_no_duplicates = _find_duplicates(
        df, [COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], COL_UPSIT
    )
    for subject_to_fix in subjects_with_multiple_edu:
        duplicate_edus = groups.get_group(subject_to_fix)
        df_no_duplicates.loc[subject_to_fix, COL_UPSIT] = duplicate_edus.mean()
    df_no_duplicates = df_no_duplicates.reset_index()
    df_no_duplicates = df_no_duplicates.sort_values(
        by=[COL_SUBJECT_TABULAR, COL_VISIT_TABULAR], key=_subject_sort_key
    )

    return df_no_duplicates
