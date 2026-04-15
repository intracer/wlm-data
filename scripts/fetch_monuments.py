#!/usr/bin/env python3
"""Fetch WLM Ukraine monuments from Wikimedia Heritage API."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wlm.fetch import fetch_monuments

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw/monuments.csv")
    args = parser.parse_args()
    fetch_monuments(args.output)
