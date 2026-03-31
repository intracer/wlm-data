#!/usr/bin/env python3
"""Download humdata Ukraine populated places Excel and convert to CSV."""

import argparse
import os
import sys
import requests
import pandas as pd
import io

HUMDATA_URL = "https://data.humdata.org/dataset/ukraine-populated-places/resource/REPLACE_ME/download/ukraine-populated-places.xlsx"

REQUIRED_COLUMNS = [
    "ADM4_EN", "ADM4_UK", "ADM4_PCODE",
    "ADM3_EN", "ADM3_UK", "ADM3_PCODE",
    "ADM2_EN", "ADM2_UK", "ADM2_PCODE",
    "ADM1_EN", "ADM1_UK", "ADM1_PCODE",
    "ADM0_EN", "ADM0_UK", "ADM0_PCODE",
    "LAT", "LON",
]


def convert(output_path: str) -> int:
    existing = "data/humdata/ukraine-populated-places.csv"
    if os.path.exists(existing):
        print(f"Using existing file {existing}", file=sys.stderr)
        df = pd.read_csv(existing)
    else:
        print(f"Downloading from {HUMDATA_URL}", file=sys.stderr)
        resp = requests.get(HUMDATA_URL, timeout=120)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}", file=sys.stderr)
    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw/humdata.csv")
    args = parser.parse_args()
    convert(args.output)
