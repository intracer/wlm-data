# Recent Changes Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a resumable batch pipeline that fetches Wikipedia/Commons recent changes for WLM-Ukraine monument and place articles, then appends them to a Delta table on Databricks Free Edition (and locally).

**Architecture:** A Python module (`src/wlm/recent_changes.py`) with three classes — `LookupSet` (builds a `(wiki, title) → source_type` map from existing CSVs), `RecentChangesClient` (paginates the MediaWiki API across 4 wiki/namespace combinations), and `RecentChangesWriter` (filters, enriches, and appends to a Delta table via PySpark). A Databricks notebook (`notebooks/recent_changes.py`) wires them together. A JSON checkpoint file in `checkpoints/` tracks the resume position between runs.

**Tech Stack:** Python 3.9+, PySpark 3.5, `requests`, `delta-spark`, pytest, MediaWiki API `action=query&list=recentchanges`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/wlm/recent_changes.py` | Create | `LookupSet`, `RecentChangesClient`, `RecentChangesWriter` |
| `notebooks/recent_changes.py` | Create | Databricks entry point |
| `tests/test_recent_changes.py` | Create | Unit tests for all three classes |
| `requirements.txt` | Modify | Add `requests>=2.31` and `delta-spark==3.2.0` |

---

## Task 1: Add dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Update requirements.txt**

Replace the contents with:

```
pyspark==3.5.6
delta-spark==3.2.0
requests>=2.31
pytest==8.3.5
```

`delta-spark==3.2.0` is the release that matches PySpark 3.5.x. `requests` is needed for HTTP calls to the MediaWiki API.

- [ ] **Step 2: Install updated dependencies**

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Expected: packages install without error. Verify with:

```bash
python -c "import delta; import requests; print('ok')"
```

Expected output: `ok`

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "deps: add delta-spark and requests for recent changes pipeline"
```

---

## Task 2: `LookupSet` — build the title lookup map

**Files:**
- Create: `src/wlm/recent_changes.py` (initial skeleton — `LookupSet` only)
- Create: `tests/test_recent_changes.py` (initial skeleton — `LookupSet` tests only)

The monuments CSV columns that matter:
- `monument_article` → `uk.wikipedia.org`, source_type `monument_article`
- `name` → `uk.wikipedia.org`, source_type `place_article` (Ukrainian place name)
- `gallery` → `commons.wikimedia.org`, source_type `commons_category`
- `commonscat` → `commons.wikimedia.org`, source_type `commons_category`
- Images CSVs column `title` (e.g. `File:WLM...jpg`) → `commons.wikimedia.org`, source_type `commons_file`

Note: English article titles are not in the current monuments CSV; `en.wikipedia.org` matching is skipped for now and the `LookupSet` only populates `uk.wikipedia.org` and `commons.wikimedia.org` entries.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_recent_changes.py`:

```python
import csv
import io
import pytest
from wlm.recent_changes import LookupSet


MONUMENTS_CSV = """\
country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction
ua,uk,80-361-0001,ua,,Вознесенська церква,,Київ,,,img.jpg,Church Cat,,,Вознесенська церква,Q1,Church Gallery,,А,1889
ua,uk,14-101-0001,ua,,Монумент Слави,,Дніпро,,,,,,,,Монумент Слави,Q2,,,А,1967
"""

IMAGES_CSV = """\
title,author,upload_date,monument_id,page_id,width,height,size_bytes,mime,camera,exif_date,categories,special_nominations,url,page_url
File:WLM UA church.jpg,User1,2024-10-01T00:00:00Z,80-361-0001,100,1000,800,500000,,,,,,
File:WLM UA monument.jpg,User2,2024-10-02T00:00:00Z,14-101-0001,101,1000,800,500000,,,,,,
"""


def _write_tmp(tmp_path, filename, content):
    p = tmp_path / filename
    p.write_text(content)
    return str(p)


def test_lookup_monument_article(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    images_paths = []
    lookup = LookupSet(monuments_path, images_paths)
    assert lookup.source_type("uk.wikipedia.org", "Вознесенська церква") == "monument_article"


def test_lookup_place_article(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    lookup = LookupSet(monuments_path, [])
    assert lookup.source_type("uk.wikipedia.org", "Київ") == "place_article"


def test_lookup_commons_category_commonscat(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    lookup = LookupSet(monuments_path, [])
    assert lookup.source_type("commons.wikimedia.org", "Church Cat") == "commons_category"


def test_lookup_commons_category_gallery(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    lookup = LookupSet(monuments_path, [])
    assert lookup.source_type("commons.wikimedia.org", "Church Gallery") == "commons_category"


def test_lookup_commons_file(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    images_path = _write_tmp(tmp_path, "images.csv", IMAGES_CSV)
    lookup = LookupSet(monuments_path, [images_path])
    assert lookup.source_type("commons.wikimedia.org", "File:WLM UA church.jpg") == "commons_file"


def test_lookup_miss_returns_none(tmp_path):
    monuments_path = _write_tmp(tmp_path, "monuments.csv", MONUMENTS_CSV)
    lookup = LookupSet(monuments_path, [])
    assert lookup.source_type("uk.wikipedia.org", "Nonexistent Article") is None


def test_lookup_deduplication_monument_article_wins(tmp_path):
    """If a title appears as both monument_article and place_article, monument_article wins."""
    csv_content = """\
country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction
ua,uk,80-361-0001,ua,,Дубль,,Київ,,,,,,,Дубль,Q1,,,А,1889
"""
    monuments_path = _write_tmp(tmp_path, "monuments.csv", csv_content)
    lookup = LookupSet(monuments_path, [])
    # "Дубль" is both name (place_article) and monument_article — monument_article wins
    assert lookup.source_type("uk.wikipedia.org", "Дубль") == "monument_article"


def test_lookup_missing_gallery_column_tolerated(tmp_path):
    """CSV without gallery column does not crash."""
    csv_content = """\
country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,commonscat,source,changed,monument_article,wd_item,registrant_id,type,year_of_construction
ua,uk,80-361-0001,ua,,Церква,,Київ,,,,,,,Церква,Q1,,А,1889
"""
    monuments_path = _write_tmp(tmp_path, "monuments.csv", csv_content)
    lookup = LookupSet(monuments_path, [])
    assert lookup.source_type("uk.wikipedia.org", "Церква") == "monument_article"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_recent_changes.py -v
```

Expected: `ImportError: cannot import name 'LookupSet' from 'wlm.recent_changes'` (module does not exist yet).

- [ ] **Step 3: Implement `LookupSet`**

Create `src/wlm/recent_changes.py`:

```python
import csv
import json
import warnings
from typing import Optional
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, LongType,
    StringType, StructField, StructType,
)


class LookupSet:
    """Builds a (wiki, title) → source_type map from monuments and images CSVs.

    Priority (highest to lowest): monument_article > commons_file >
    commons_category > place_article. This means if the same title appears
    under multiple source types, the highest-priority type is kept.
    """

    _PRIORITY = {
        "monument_article": 0,
        "commons_file": 1,
        "commons_category": 2,
        "place_article": 3,
    }

    def __init__(self, monuments_path: str, images_paths: list[str]):
        self._map: dict[tuple[str, str], str] = {}
        self._load_monuments(monuments_path)
        for p in images_paths:
            self._load_images(p)

    def _set(self, wiki: str, title: str, source_type: str) -> None:
        if not title:
            return
        key = (wiki, title)
        existing = self._map.get(key)
        if existing is None or self._PRIORITY[source_type] < self._PRIORITY[existing]:
            self._map[key] = source_type

    def _load_monuments(self, path: str) -> None:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # place_article (lowest priority — add first so higher-priority types overwrite)
                self._set("uk.wikipedia.org", row.get("name", ""), "place_article")
                # monument_article
                self._set("uk.wikipedia.org", row.get("monument_article", ""), "monument_article")
                # commons_category
                self._set("commons.wikimedia.org", row.get("commonscat", ""), "commons_category")
                if "gallery" in reader.fieldnames:
                    self._set("commons.wikimedia.org", row.get("gallery", ""), "commons_category")
                else:
                    warnings.warn("monuments CSV has no 'gallery' column — skipping")

    def _load_images(self, path: str) -> None:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self._set("commons.wikimedia.org", row.get("title", ""), "commons_file")

    def source_type(self, wiki: str, title: str) -> Optional[str]:
        return self._map.get((wiki, title))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_recent_changes.py -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/recent_changes.py tests/test_recent_changes.py
git commit -m "feat: add LookupSet for recent changes pipeline"
```

---

## Task 3: `RecentChangesClient` — fetch and paginate the API

**Files:**
- Modify: `src/wlm/recent_changes.py` — add `RecentChangesClient`
- Modify: `tests/test_recent_changes.py` — add client tests

The MediaWiki API endpoint: `https://{wiki}/w/api.php`

Parameters used:
- `action=query`
- `list=recentchanges`
- `rcnamespace={ns}` — namespace filter
- `rcprop=ids|title|type|user|userid|comment|parsedcomment|timestamp|sizes|redirect|tags|sha1|loginfo`
- `rclimit=500` — maximum per page
- `rcstart={since}` — ISO 8601 timestamp (start from this time, most-recent first)
- `rccontinue={token}` — pagination token from previous page

The 4 wiki/namespace combinations to fetch:

| wiki | ns | source_types |
|---|---|---|
| `uk.wikipedia.org` | `0` | monument_article, place_article |
| `en.wikipedia.org` | `0` | monument_article, place_article |
| `commons.wikimedia.org` | `14` | commons_category |
| `commons.wikimedia.org` | `6` | commons_file |

Checkpoint file format: `{"rccontinue": "<token>", "timestamp": "<ISO8601>"}`. On first run (no file), `rcstart` is omitted (API defaults to most recent changes).

The client adds a `wiki` key to each raw record before returning.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_recent_changes.py`:

```python
import json
import os
import tempfile
from unittest.mock import patch, MagicMock
from wlm.recent_changes import RecentChangesClient


def _mock_response(data: dict) -> MagicMock:
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = data
    return m


def test_client_fetches_single_page(tmp_path):
    """Single page response (no continue token) returns records with wiki field."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    page1 = {
        "query": {
            "recentchanges": [
                {"rcid": 1, "title": "Церква", "ns": 0, "type": "edit",
                 "timestamp": "2024-01-01T00:00:00Z", "user": "u", "userid": 1,
                 "comment": "", "parsedcomment": "", "sizes": {"old": 100, "new": 200},
                 "revid": 10, "old_revid": 9, "pageid": 5, "redirect": False,
                 "tags": [], "sha1": "abc", "logtype": "", "logaction": "", "logparams": {}},
            ]
        }
    }
    with patch("wlm.recent_changes.requests.get", return_value=_mock_response(page1)):
        client = RecentChangesClient(checkpoint_path)
        records = client.fetch()
    church_records = [r for r in records if r["title"] == "Церква"]
    assert len(church_records) >= 1
    assert church_records[0]["wiki"] == "uk.wikipedia.org"


def test_client_paginates(tmp_path):
    """Client follows rccontinue until no more pages."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    page1 = {
        "continue": {"rccontinue": "TOKEN|1"},
        "query": {"recentchanges": [
            {"rcid": 1, "title": "A", "ns": 0, "type": "edit",
             "timestamp": "2024-01-01T00:00:00Z", "user": "u", "userid": 1,
             "comment": "", "parsedcomment": "", "sizes": {"old": 0, "new": 10},
             "revid": 1, "old_revid": 0, "pageid": 1, "redirect": False,
             "tags": [], "sha1": "x", "logtype": "", "logaction": "", "logparams": {}},
        ]},
    }
    page2 = {
        "query": {"recentchanges": [
            {"rcid": 2, "title": "B", "ns": 0, "type": "new",
             "timestamp": "2024-01-01T01:00:00Z", "user": "v", "userid": 2,
             "comment": "", "parsedcomment": "", "sizes": {"old": 0, "new": 20},
             "revid": 2, "old_revid": 0, "pageid": 2, "redirect": False,
             "tags": [], "sha1": "y", "logtype": "", "logaction": "", "logparams": {}},
        ]},
    }
    responses = [_mock_response(page1), _mock_response(page2)] * 4
    with patch("wlm.recent_changes.requests.get", side_effect=responses):
        client = RecentChangesClient(checkpoint_path)
        records = client.fetch()
    titles = {r["title"] for r in records}
    assert "A" in titles
    assert "B" in titles


def test_client_reads_checkpoint(tmp_path):
    """When checkpoint exists, rcstart is read from it."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    with open(checkpoint_path, "w") as f:
        json.dump({"rccontinue": "", "timestamp": "2024-06-01T12:00:00Z"}, f)
    page = {"query": {"recentchanges": []}}
    calls = []
    def fake_get(url, params=None, **kwargs):
        calls.append(params or {})
        return _mock_response(page)
    with patch("wlm.recent_changes.requests.get", side_effect=fake_get):
        client = RecentChangesClient(checkpoint_path)
        client.fetch()
    assert any(p.get("rcstart") == "2024-06-01T12:00:00Z" for p in calls)


def test_client_since_overrides_checkpoint(tmp_path):
    """--since parameter overrides checkpoint timestamp."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    with open(checkpoint_path, "w") as f:
        json.dump({"rccontinue": "", "timestamp": "2024-01-01T00:00:00Z"}, f)
    page = {"query": {"recentchanges": []}}
    calls = []
    def fake_get(url, params=None, **kwargs):
        calls.append(params or {})
        return _mock_response(page)
    with patch("wlm.recent_changes.requests.get", side_effect=fake_get):
        client = RecentChangesClient(checkpoint_path, since="2024-09-01T00:00:00Z")
        client.fetch()
    assert any(p.get("rcstart") == "2024-09-01T00:00:00Z" for p in calls)


def test_client_raises_on_api_error(tmp_path):
    """API error field causes RuntimeError."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    error_response = {"error": {"code": "invalidparam", "info": "Bad param"}}
    with patch("wlm.recent_changes.requests.get", return_value=_mock_response(error_response)):
        client = RecentChangesClient(checkpoint_path)
        with pytest.raises(RuntimeError, match="Bad param"):
            client.fetch()


def test_client_returns_last_continue_token(tmp_path):
    """fetch() returns the last rccontinue token seen for checkpoint use."""
    checkpoint_path = str(tmp_path / "checkpoint.json")
    page = {
        "continue": {"rccontinue": "FINAL_TOKEN|99"},
        "query": {"recentchanges": []},
    }
    # Second call has no continue (end of pages)
    page2 = {"query": {"recentchanges": []}}
    responses = [_mock_response(page), _mock_response(page2)] * 4
    with patch("wlm.recent_changes.requests.get", side_effect=responses):
        client = RecentChangesClient(checkpoint_path)
        _, token = client.fetch_with_token()
    assert token == "FINAL_TOKEN|99"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_recent_changes.py::test_client_fetches_single_page -v
```

Expected: `ImportError` or `AttributeError` — `RecentChangesClient` not defined yet.

- [ ] **Step 3: Implement `RecentChangesClient`**

Add to `src/wlm/recent_changes.py` (after the `LookupSet` class):

```python
_WIKI_NS_COMBOS = [
    ("uk.wikipedia.org", "0"),
    ("en.wikipedia.org", "0"),
    ("commons.wikimedia.org", "14"),
    ("commons.wikimedia.org", "6"),
]

_RC_PROPS = "ids|title|type|user|userid|comment|parsedcomment|timestamp|sizes|redirect|tags|sha1|loginfo"


class RecentChangesClient:
    """Fetches recent changes from the MediaWiki API across 4 wiki/namespace combos.

    Does NOT update the checkpoint — that is RecentChangesWriter's responsibility.
    """

    def __init__(self, checkpoint_path: str, since: Optional[str] = None):
        self._checkpoint_path = checkpoint_path
        self._since = since

    def _effective_since(self) -> Optional[str]:
        if self._since:
            return self._since
        if not os.path.exists(self._checkpoint_path):
            return None
        with open(self._checkpoint_path) as f:
            data = json.load(f)
        return data.get("timestamp") or None

    def fetch(self) -> list[dict]:
        records, _ = self.fetch_with_token()
        return records

    def fetch_with_token(self) -> tuple[list[dict], Optional[str]]:
        all_records: list[dict] = []
        last_token: Optional[str] = None
        since = self._effective_since()

        for wiki, ns in _WIKI_NS_COMBOS:
            params: dict = {
                "action": "query",
                "list": "recentchanges",
                "rcnamespace": ns,
                "rcprop": _RC_PROPS,
                "rclimit": "500",
                "format": "json",
            }
            if since:
                params["rcstart"] = since

            while True:
                resp = requests.get(f"https://{wiki}/w/api.php", params=params)
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    raise RuntimeError(data["error"].get("info", str(data["error"])))

                for rc in data.get("query", {}).get("recentchanges", []):
                    rc["wiki"] = wiki
                    all_records.append(rc)

                if "continue" in data:
                    token = data["continue"]["rccontinue"]
                    last_token = token
                    params["rccontinue"] = token
                else:
                    break

        return all_records, last_token
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_recent_changes.py -v -k "client"
```

Expected: all 6 client tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/recent_changes.py tests/test_recent_changes.py
git commit -m "feat: add RecentChangesClient with pagination and checkpoint resume"
```

---

## Task 4: `RecentChangesWriter` — filter, enrich, and append to Delta

**Files:**
- Modify: `src/wlm/recent_changes.py` — add `RC_SCHEMA` constant and `RecentChangesWriter`
- Modify: `tests/test_recent_changes.py` — add writer tests

The writer:
1. Filters raw records to those where `lookup.source_type(wiki, title)` is not None
2. Flattens `sizes` dict to `old_len` / `new_len`; JSON-encodes `logparams`; converts `tags` to list
3. Creates a PySpark DataFrame using `RC_SCHEMA`
4. Appends to Delta table at the given output path
5. Writes the new checkpoint: `{"rccontinue": token, "timestamp": <most recent timestamp from records>}`

If no records matched, step 4 is skipped but the checkpoint is still updated.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_recent_changes.py`:

```python
import shutil
from wlm.recent_changes import RecentChangesWriter, LookupSet


def _make_record(wiki, title, rcid=1, source_type_hint=None):
    return {
        "wiki": wiki,
        "rcid": rcid,
        "title": title,
        "type": "edit",
        "ns": 0,
        "timestamp": "2024-10-01T12:00:00Z",
        "user": "TestUser",
        "userid": 42,
        "comment": "test edit",
        "parsedcomment": "test edit",
        "sizes": {"old": 100, "new": 200},
        "revid": 10,
        "old_revid": 9,
        "pageid": 5,
        "redirect": False,
        "tags": ["mobile edit"],
        "sha1": "deadbeef",
        "logtype": "",
        "logaction": "",
        "logparams": {},
    }


def test_writer_filters_unmatched(spark, tmp_path):
    """Records not in LookupSet are excluded from the Delta table."""
    monuments_csv = tmp_path / "monuments.csv"
    monuments_csv.write_text(
        "country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,"
        "commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction\n"
        "ua,uk,80-361-0001,ua,,Київ,,Київ,,,,,,,Церква,Q1,,,А,1889\n"
    )
    lookup = LookupSet(str(monuments_csv), [])
    records = [
        _make_record("uk.wikipedia.org", "Церква", rcid=1),    # matched
        _make_record("uk.wikipedia.org", "Unrelated", rcid=2),  # not matched
    ]
    output_path = str(tmp_path / "delta_out")
    checkpoint_path = str(tmp_path / "checkpoint.json")
    writer = RecentChangesWriter(spark, output_path, checkpoint_path)
    writer.write(records, lookup, last_token=None)

    result = spark.read.format("delta").load(output_path)
    titles = {row.title for row in result.collect()}
    assert titles == {"Церква"}


def test_writer_appends_across_runs(spark, tmp_path):
    """Second write appends rows — does not overwrite."""
    monuments_csv = tmp_path / "monuments.csv"
    monuments_csv.write_text(
        "country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,"
        "commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction\n"
        "ua,uk,80-361-0001,ua,,Київ,,Київ,,,,,,,Церква,Q1,,,А,1889\n"
        "ua,uk,80-361-0002,ua,,Дніпро,,Дніпро,,,,,,,Собор,Q2,,,А,1900\n"
    )
    lookup = LookupSet(str(monuments_csv), [])
    output_path = str(tmp_path / "delta_out")
    checkpoint_path = str(tmp_path / "checkpoint.json")
    writer = RecentChangesWriter(spark, output_path, checkpoint_path)

    writer.write([_make_record("uk.wikipedia.org", "Церква", rcid=1)], lookup, last_token=None)
    writer.write([_make_record("uk.wikipedia.org", "Собор", rcid=2)], lookup, last_token=None)

    result = spark.read.format("delta").load(output_path)
    assert result.count() == 2


def test_writer_updates_checkpoint(spark, tmp_path):
    """Checkpoint file is written after successful write."""
    monuments_csv = tmp_path / "monuments.csv"
    monuments_csv.write_text(
        "country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,"
        "commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction\n"
        "ua,uk,80-361-0001,ua,,Київ,,Київ,,,,,,,Церква,Q1,,,А,1889\n"
    )
    lookup = LookupSet(str(monuments_csv), [])
    output_path = str(tmp_path / "delta_out")
    checkpoint_path = str(tmp_path / "checkpoint.json")
    writer = RecentChangesWriter(spark, output_path, checkpoint_path)
    writer.write(
        [_make_record("uk.wikipedia.org", "Церква", rcid=1)],
        lookup,
        last_token="TOKEN|42",
    )
    with open(checkpoint_path) as f:
        cp = json.load(f)
    assert cp["rccontinue"] == "TOKEN|42"
    assert cp["timestamp"] == "2024-10-01T12:00:00Z"


def test_writer_skips_delta_write_on_empty_match(spark, tmp_path):
    """Empty match set: no Delta table created, checkpoint still updated."""
    monuments_csv = tmp_path / "monuments.csv"
    monuments_csv.write_text(
        "country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,"
        "commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction\n"
    )
    lookup = LookupSet(str(monuments_csv), [])
    output_path = str(tmp_path / "delta_out")
    checkpoint_path = str(tmp_path / "checkpoint.json")
    writer = RecentChangesWriter(spark, output_path, checkpoint_path)
    writer.write(
        [_make_record("uk.wikipedia.org", "Nonexistent", rcid=1)],
        lookup,
        last_token="TOKEN|0",
    )
    assert not os.path.exists(output_path)
    with open(checkpoint_path) as f:
        cp = json.load(f)
    assert cp["rccontinue"] == "TOKEN|0"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_recent_changes.py -v -k "writer"
```

Expected: `ImportError` — `RecentChangesWriter` not defined yet.

- [ ] **Step 3: Implement `RC_SCHEMA` and `RecentChangesWriter`**

Add to `src/wlm/recent_changes.py` (after `RecentChangesClient`):

```python
RC_SCHEMA = StructType([
    StructField("rcid", LongType(), True),
    StructField("type", StringType(), True),
    StructField("ns", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user", StringType(), True),
    StructField("userid", LongType(), True),
    StructField("comment", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("old_len", IntegerType(), True),
    StructField("new_len", IntegerType(), True),
    StructField("revid", LongType(), True),
    StructField("old_revid", LongType(), True),
    StructField("pageid", LongType(), True),
    StructField("redirect", BooleanType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("sha1", StringType(), True),
    StructField("logtype", StringType(), True),
    StructField("logaction", StringType(), True),
    StructField("logparams", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("source_type", StringType(), True),
])


def _flatten_record(record: dict, source_type: str) -> dict:
    sizes = record.get("sizes") or {}
    return {
        "rcid": record.get("rcid"),
        "type": record.get("type"),
        "ns": record.get("ns"),
        "title": record.get("title"),
        "timestamp": record.get("timestamp"),
        "user": record.get("user"),
        "userid": record.get("userid"),
        "comment": record.get("comment"),
        "parsedcomment": record.get("parsedcomment"),
        "old_len": sizes.get("old"),
        "new_len": sizes.get("new"),
        "revid": record.get("revid"),
        "old_revid": record.get("old_revid"),
        "pageid": record.get("pageid"),
        "redirect": record.get("redirect"),
        "tags": record.get("tags") or [],
        "sha1": record.get("sha1"),
        "logtype": record.get("logtype"),
        "logaction": record.get("logaction"),
        "logparams": json.dumps(record.get("logparams") or {}),
        "wiki": record.get("wiki"),
        "source_type": source_type,
    }


class RecentChangesWriter:
    """Filters, enriches, and appends matched recent-change records to a Delta table."""

    def __init__(self, spark: SparkSession, output_path: str, checkpoint_path: str):
        self._spark = spark
        self._output_path = output_path
        self._checkpoint_path = checkpoint_path

    def write(
        self,
        records: list[dict],
        lookup: LookupSet,
        last_token: Optional[str],
    ) -> None:
        matched = []
        latest_ts: Optional[str] = None
        for rec in records:
            st = lookup.source_type(rec.get("wiki", ""), rec.get("title", ""))
            if st is not None:
                matched.append(_flatten_record(rec, st))
            ts = rec.get("timestamp")
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts

        if matched:
            df = self._spark.createDataFrame(matched, schema=RC_SCHEMA)
            df.write.format("delta").mode("append").save(self._output_path)

        self._write_checkpoint(last_token or "", latest_ts or "")

    def _write_checkpoint(self, token: str, timestamp: str) -> None:
        with open(self._checkpoint_path, "w") as f:
            json.dump({"rccontinue": token, "timestamp": timestamp}, f)
```

- [ ] **Step 4: Configure Spark session for Delta in conftest**

The existing `conftest.py` creates a plain Spark session. Delta requires extra config. Update `tests/conftest.py`:

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local")
               .appName("wlm-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .getOrCreate())
    yield session
    session.stop()
```

- [ ] **Step 5: Run all tests to verify they pass**

```bash
pytest tests/ -v
```

Expected: all tests PASS (existing monuments/images tests + new recent_changes tests).

- [ ] **Step 6: Commit**

```bash
git add src/wlm/recent_changes.py tests/test_recent_changes.py tests/conftest.py
git commit -m "feat: add RecentChangesWriter and Delta schema"
```

---

## Task 5: Databricks notebook

**Files:**
- Create: `notebooks/recent_changes.py`

This notebook follows the same pattern as `notebooks/monuments.py` and `notebooks/images_streaming.py`: a `# Databricks notebook source` header, `# COMMAND ----------` cell separators, configurable DBFS paths, and `spark` pre-created by Databricks.

- [ ] **Step 1: Create `notebooks/recent_changes.py`**

```python
# Databricks notebook source
# This notebook runs the recent changes pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - CSV files uploaded to DBFS at the paths below
#   - Run each time you want to ingest new recent changes
#     (each run continues from where the last one left off via checkpoints/)

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

# COMMAND ----------

from wlm.recent_changes import LookupSet, RecentChangesClient, RecentChangesWriter

# COMMAND ----------
# Config — edit DBFS paths as needed

MONUMENTS_CSV   = "dbfs:/Volumes/workspace/default/wlm_data/monuments/wlm-ua-monuments.csv"
IMAGES_GLOB     = "dbfs:/Volumes/workspace/default/wlm_data/images"
CHECKPOINT_PATH = "checkpoints/recent_changes.json"
OUTPUT_PATH     = "dbfs:/tables/recent_changes"

# Optional: override resume timestamp (ISO 8601). Leave as None to use checkpoint.
SINCE = None  # e.g. "2024-09-01T00:00:00Z"

# COMMAND ----------
# Build the images file list from DBFS

import glob as _glob
images_paths = _glob.glob(f"{IMAGES_GLOB}/wlm-UA-*-images.csv")

# COMMAND ----------
# Build lookup set from monuments and images CSVs

lookup = LookupSet(MONUMENTS_CSV, images_paths)
print(f"Lookup set: {len(lookup._map)} entries")

# COMMAND ----------
# Fetch recent changes from MediaWiki API

client = RecentChangesClient(CHECKPOINT_PATH, since=SINCE)
records, last_token = client.fetch_with_token()
print(f"Fetched {len(records)} raw records")

# COMMAND ----------
# Filter, enrich, and append matched records to Delta table

writer = RecentChangesWriter(spark, OUTPUT_PATH, CHECKPOINT_PATH)
writer.write(records, lookup, last_token)
print(f"Done. Checkpoint updated.")

# COMMAND ----------
# Preview the Delta table

spark.read.format("delta").load(OUTPUT_PATH).orderBy("timestamp", ascending=False).show(20, truncate=False)
```

- [ ] **Step 2: Verify notebook syntax**

```bash
python -c "import ast; ast.parse(open('notebooks/recent_changes.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 3: Commit**

```bash
git add notebooks/recent_changes.py
git commit -m "feat: add recent changes Databricks notebook"
```

---

## Task 6: Local smoke test

**Files:** none — this task verifies the full pipeline runs end-to-end locally using real data files.

- [ ] **Step 1: Run the full pipeline locally**

```bash
source .venv/bin/activate
python - <<'EOF'
import sys
sys.path.insert(0, "src")
import glob
from pyspark.sql import SparkSession
from wlm.recent_changes import LookupSet, RecentChangesClient, RecentChangesWriter

spark = (SparkSession.builder
         .master("local")
         .appName("rc-smoke")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

images_paths = glob.glob("data/images/wlm-UA-*-images.csv")
lookup = LookupSet("data/wiki/monuments/wlm-ua-monuments.csv", images_paths)
print(f"Lookup entries: {len(lookup._map)}")

client = RecentChangesClient("checkpoints/recent_changes.json")
records, token = client.fetch_with_token()
print(f"Raw records: {len(records)}")

writer = RecentChangesWriter(spark, "output/recent_changes", "checkpoints/recent_changes.json")
writer.write(records, lookup, token)
print("Write complete.")

spark.read.format("delta").load("output/recent_changes").show(10, truncate=False)
spark.stop()
EOF
```

Expected: script runs without error, prints lookup entry count, raw record count, and a table preview. `checkpoints/recent_changes.json` is created/updated.

- [ ] **Step 2: Verify checkpoint was created**

```bash
cat checkpoints/recent_changes.json
```

Expected: JSON with `rccontinue` and `timestamp` fields set.

- [ ] **Step 3: Run a second time to verify resume**

Run the same script again. Expected: the new run's `rcstart` matches the timestamp from step 2, and only changes since that timestamp are fetched.

- [ ] **Step 4: Commit checkpoint stub (optional)**

If `checkpoints/recent_changes.json` is not already gitignored and you don't want to commit it:

```bash
echo "checkpoints/recent_changes.json" >> .gitignore
git add .gitignore
git commit -m "chore: gitignore recent changes checkpoint"
```
