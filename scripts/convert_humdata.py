#!/usr/bin/env python3
"""Download humdata Ukraine populated places Excel and convert to CSV."""

import argparse
import io
import sys

import pandas as pd
import requests
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

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
    existing = REPO_ROOT / "data/humdata/ukraine-populated-places.csv"
    if existing.exists():
        print(f"Using existing file {existing}", file=sys.stderr)
        df = pd.read_csv(existing)
    else:
        if "REPLACE_ME" in HUMDATA_URL:
            raise RuntimeError(
                "HUMDATA_URL contains a placeholder. Update the URL in convert_humdata.py "
                "or place the CSV at data/humdata/ukraine-populated-places.csv"
            )
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
