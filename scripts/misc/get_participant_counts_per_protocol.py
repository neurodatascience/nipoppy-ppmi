#!/usr/bin/env python

import argparse
import json
import re
from pathlib import Path

import pandas as pd

DEFAULT_FPATH_LONI = Path(__file__).parent / "idaSearch.csv"
DEFAULT_FPATH_PROTOCOLS = Path(
    "/lustre06/project/6018311/mwang8/projects/nipoppy-ppmi/nipoppy/workflow/tabular/ppmi_imaging_descriptions.json"
)
DEFAULT_PROTOCOL_FILTER = "dwi"
DEFAULT_VISIT_FILTER = [
    "Baseline",
    "Month 12",
    "Month 24",
    "Month 36",
    "Month 48",
]
DEFAULT_FPATH_OUT = Path(__file__).parent / "participant_counts_per_protocol.csv"

VALID_FILTERS = ["dwi", "func", "anat", "T1w", "T2w", "T2starw", "FLAIR"]
COL_LONI_PROTOCOL = "Description"
COL_LONI_VISIT = "Visit"
RE_NEUROMELANIN = '([nN][mM])|([gG][rR][eE].*[mM][tT])' # neuromelanin pattern


def get_filtered_protocols(protocols_map: dict, protocol_filter: str) -> set:
    def _get_filtered_protocols(
        protocols_map: dict, protocol_filter: str | None, protocols: set
    ):
        for key, value in protocols_map.items():
            if key == protocol_filter or protocol_filter is None:
                if isinstance(value, list):
                    protocols.update(value)
                elif isinstance(value, dict):
                    protocols.update(_get_filtered_protocols(value, None, protocols))
            elif isinstance(value, dict):
                protocols = _get_filtered_protocols(value, protocol_filter, protocols)
        return protocols

    protocols = _get_filtered_protocols(protocols_map, protocol_filter, set())
    if len(protocols) == 0:
        raise RuntimeError(f"No protocols found for filter {protocol_filter}")
    return protocols


def get_participant_counts_per_protocol(
    fpath_loni=DEFAULT_FPATH_LONI,
    fpath_protocols=DEFAULT_FPATH_PROTOCOLS,
    protocol_filter=DEFAULT_PROTOCOL_FILTER,
    visit_filter=DEFAULT_VISIT_FILTER,
    fpath_out=DEFAULT_FPATH_OUT,
    neuromelanin=False,
):

    df_loni = pd.read_csv(fpath_loni)
    print(f"Loaded LONI file:\n{df_loni}")

    protocols_map: dict = json.loads(fpath_protocols.read_text())
    print(f"\nLoaded protocols file with keys {list(protocols_map.keys())}")

    protocols = get_filtered_protocols(protocols_map, protocol_filter)
    print(
        f'\nFound {len(protocols)} protocols after applying filter "{protocol_filter}"'
    )
    if neuromelanin:
        protocols = {protocol for protocol in protocols if re.search(RE_NEUROMELANIN, protocol)}
        print(f'\nFound {len(protocols)} neuromelanin protocols')
    # for protocol in sorted(list(protocols)):
    #     print(f"\t{protocol}")

    df_loni_filtered = df_loni[df_loni[COL_LONI_PROTOCOL].isin(protocols)]
    print(f"\nFiltered protocols in LONI file:\n{df_loni_filtered}")

    df_loni_filtered = df_loni_filtered[
        df_loni_filtered[COL_LONI_VISIT].isin(visit_filter)
    ]
    print(f"\nFiltered visits in LONI file:\n{df_loni_filtered}")

    df_counts = (
        df_loni_filtered.groupby(COL_LONI_PROTOCOL).size().sort_values(ascending=False)
    ).rename("Count")
    df_counts.index.rename("Protocol", inplace=True)

    print(f"\nParticipant counts per protocol:\n{df_counts}")
    df_counts.to_csv(fpath_out)
    print(f"\nSaved participant counts per protocol to {fpath_out}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Get participant counts per protocol")
    parser.add_argument(
        "--loni-csv-file",
        type=Path,
        default=DEFAULT_FPATH_LONI,
        help=f"Path to LONI IDA search results file (default: {DEFAULT_FPATH_LONI})",
    )
    parser.add_argument(
        "--protocol-json-file",
        type=Path,
        default=DEFAULT_FPATH_PROTOCOLS,
        help=f"Path to protocol JSON file generated by custom Nipoppy PPMI script (default: {DEFAULT_FPATH_PROTOCOLS})",
    )
    parser.add_argument(
        "--protocol-filter",
        type=str,
        default="dwi",
        choices=VALID_FILTERS,
        help=f"Filter for protocol names (default: {DEFAULT_PROTOCOL_FILTER})",
    )
    parser.add_argument(
        "--visit-filter",
        type=list,
        default=DEFAULT_VISIT_FILTER,
        help=f"Filter for session names (default: {DEFAULT_VISIT_FILTER})",
        nargs="+",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_FPATH_OUT,
        help=f"Path to output file (default: {DEFAULT_FPATH_OUT})",
    )
    parser.add_argument(
        '--neuromelanin',
        action='store_true',
        help='Keep only neuromelanin protocols',
    )

    args = parser.parse_args()
    get_participant_counts_per_protocol(
        fpath_loni=args.loni_csv_file,
        fpath_protocols=args.protocol_json_file,
        protocol_filter=args.protocol_filter,
        visit_filter=args.visit_filter,
        fpath_out=args.out,
        neuromelanin=args.neuromelanin,
    )
