#!/usr/bin/env python3
"""Download humdata Ukraine populated places Excel and convert to CSV."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wlm.fetch import fetch_humdata

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw/humdata.csv")
    args = parser.parse_args()
    fetch_humdata(args.output)
