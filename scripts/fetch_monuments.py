#!/usr/bin/env python3
"""Fetch WLM Ukraine monuments from Wikimedia Heritage API."""

import argparse
import csv
import io
import sys
import requests

HERITAGE_API = (
    "https://tools.wmflabs.org/heritage/api/api.php"
    "?action=search&srcountry=ua&srlang=uk&format=csv&limit=10000"
)

EXPECTED_COLUMNS = [
    "country", "lang", "id", "adm0", "adm2", "name", "address",
    "municipality", "lat", "lon", "image", "commonscat", "source",
    "changed", "monument_article", "wd_item", "gallery",
    "registrant_id", "type", "year_of_construction",
]


def fetch(output_path: str) -> int:
    print(f"Fetching monuments from {HERITAGE_API}", file=sys.stderr)
    resp = requests.get(HERITAGE_API, timeout=120)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)

    if not rows:
        raise ValueError("API returned 0 rows — check endpoint")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} monuments to {output_path}", file=sys.stderr)
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw/monuments.csv")
    args = parser.parse_args()
    fetch(args.output)
