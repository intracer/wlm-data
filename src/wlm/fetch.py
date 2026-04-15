"""Fetch and convert raw input data for the WLM Ukraine pipeline."""

import csv
import io
import sys
from pathlib import Path

import pandas as pd
import requests

HERITAGE_API = (
    "https://tools.wmflabs.org/heritage/api/api.php"
    "?action=search&srcountry=ua&srlang=uk&format=csv&limit=10000"
)

MONUMENTS_COLUMNS = [
    "country", "lang", "id", "adm0", "adm2", "name", "address",
    "municipality", "lat", "lon", "image", "commonscat", "source",
    "changed", "monument_article", "wd_item", "gallery",
    "registrant_id", "type", "year_of_construction",
]

HUMDATA_REQUIRED_COLUMNS = [
    "ADM4_EN", "ADM4_UK", "ADM4_PCODE",
    "ADM3_EN", "ADM3_UK", "ADM3_PCODE",
    "ADM2_EN", "ADM2_UK", "ADM2_PCODE",
    "ADM1_EN", "ADM1_UK", "ADM1_PCODE",
    "ADM0_EN", "ADM0_UK", "ADM0_PCODE",
    "LAT", "LON",
]

# Local fallback used when the remote URL is unavailable or unreplaced
_HUMDATA_FALLBACK = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "humdata" / "ukraine-populated-places.csv"
)

# Replace REPLACE_ME with the actual Humdata resource UUID to enable remote download
HUMDATA_URL = (
    "https://data.humdata.org/dataset/ukraine-populated-places"
    "/resource/REPLACE_ME/download/ukraine-populated-places.xlsx"
)


def fetch_monuments(output_path: str) -> int:
    """Fetch WLM Ukraine monuments from the Wikimedia Heritage API and write to CSV.

    Returns the number of rows written.
    """
    print(f"Fetching monuments from {HERITAGE_API}", file=sys.stderr)
    resp = requests.get(HERITAGE_API, timeout=120)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)

    if not rows:
        raise ValueError("Heritage API returned 0 rows — check endpoint")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MONUMENTS_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} monuments to {output_path}", file=sys.stderr)
    return len(rows)


def fetch_humdata(output_path: str) -> int:
    """Convert humdata Ukraine populated places to CSV.

    Uses the local fallback at data/humdata/ukraine-populated-places.csv if it exists,
    otherwise downloads from HUMDATA_URL (which requires replacing the REPLACE_ME
    placeholder with the actual resource UUID).

    Returns the number of rows written.
    """
    if _HUMDATA_FALLBACK.exists():
        print(f"Using existing file {_HUMDATA_FALLBACK}", file=sys.stderr)
        df = pd.read_csv(_HUMDATA_FALLBACK)
    else:
        if "REPLACE_ME" in HUMDATA_URL:
            raise RuntimeError(
                "HUMDATA_URL contains a placeholder. Update the URL in src/wlm/fetch.py "
                "or place the CSV at data/humdata/ukraine-populated-places.csv"
            )
        print(f"Downloading from {HUMDATA_URL}", file=sys.stderr)
        resp = requests.get(HUMDATA_URL, timeout=120)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))

    missing = [c for c in HUMDATA_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}", file=sys.stderr)
    return len(df)
