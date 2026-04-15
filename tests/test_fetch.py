"""Integration tests for src/wlm/fetch.py.

Run with:
    pytest tests/test_fetch.py -v -m integration

These tests hit live network endpoints and/or read local data files.
They are excluded from the default fast test suite (run without -m integration).
"""

import csv
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.integration
def test_fetch_monuments_returns_rows(tmp_path):
    """fetch_monuments hits the Heritage API and writes a non-empty CSV."""
    from wlm.fetch import MONUMENTS_COLUMNS, fetch_monuments

    out = tmp_path / "monuments.csv"
    count = fetch_monuments(str(out))

    assert count > 0, "Heritage API returned 0 rows"
    assert out.exists(), "Output file was not created"

    with open(out, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == count
    assert set(MONUMENTS_COLUMNS).issubset(set(rows[0].keys()))


@pytest.mark.integration
def test_fetch_monuments_columns(tmp_path):
    """Fetched monuments CSV contains all expected columns."""
    from wlm.fetch import MONUMENTS_COLUMNS, fetch_monuments

    out = tmp_path / "monuments.csv"
    fetch_monuments(str(out))

    with open(out, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))

    for col in MONUMENTS_COLUMNS:
        assert col in header, f"Missing column: {col}"


@pytest.mark.integration
def test_fetch_humdata_uses_local_fallback(tmp_path):
    """fetch_humdata reads the local CSV fallback when it exists."""
    from wlm.fetch import HUMDATA_REQUIRED_COLUMNS, fetch_humdata

    out = tmp_path / "humdata.csv"
    # Local fallback at data/humdata/ukraine-populated-places.csv is checked in
    count = fetch_humdata(str(out))

    assert count > 0, "fetch_humdata returned 0 rows from local fallback"
    assert out.exists(), "Output file was not created"

    import pandas as pd
    df = pd.read_csv(out)
    for col in HUMDATA_REQUIRED_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


@pytest.mark.integration
def test_fetch_humdata_row_count_matches(tmp_path):
    """Row count returned by fetch_humdata matches the rows written to disk."""
    from wlm.fetch import fetch_humdata

    out = tmp_path / "humdata.csv"
    count = fetch_humdata(str(out))

    import pandas as pd
    df = pd.read_csv(out)
    assert len(df) == count
