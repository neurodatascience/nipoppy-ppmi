#!/usr/bin/env python
"""Update the PPMI doughnut file inside the HeuDiConv 0.12.2 container."""

import argparse
from pathlib import Path

import pandas as pd

COL_DPATH_MAPPING = {
    "in_pre_reorg": "{dpath_root}/sourcedata/imaging/pre_reorg/{participant_dicom_dir}",
    "in_post_reorg": "{dpath_root}/sourcedata/imaging/post_reorg/sub-{participant_id}/ses-{session_id}",
    "in_bids": "{dpath_root}/bids/sub-{participant_id}/ses-{session_id}",
}


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dpath-root",
        type=Path,
        required=True,
        help="Path to the root directory of the dataset.",
    )
    parser.add_argument(
        "--fpath-doughnut",
        type=Path,
        required=True,
        help="Path to an existing doughnut file. Must be up-to-date.",
    )
    parser.add_argument("--fpath-out", type=Path, help="Path to output doughnut file.")
    return parser


def check_status(dpath_pattern: str, **kwargs):
    dpath_to_check = Path(dpath_pattern.format(**kwargs))
    # print(f'Checking directory {dpath_to_check}: {next(dpath_to_check.iterdir(), None)}')
    return dpath_to_check.exists() and next(dpath_to_check.iterdir(), None) is not None


def run(
    dpath_root: Path,
    fpath_doughnut: Path,
    fpath_out: Path,
):
    df_doughnut = pd.read_csv(fpath_doughnut, sep="\t", dtype=str)
    for index, row in df_doughnut.iterrows():
        for col, dpath_pattern in COL_DPATH_MAPPING.items():
            df_doughnut.loc[index, col] = check_status(
                dpath_pattern=dpath_pattern,
                dpath_root=dpath_root,
                participant_id=row["participant_id"],
                session_id=row["session_id"],
                participant_dicom_dir=row["participant_dicom_dir"],
            )

    print(df_doughnut)
    df_doughnut.to_csv(fpath_out, sep="\t", index=False)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    run(
        dpath_root=args.dpath_root,
        fpath_doughnut=args.fpath_doughnut,
        fpath_out=args.fpath_out,
    )
