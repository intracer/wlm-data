import csv
import io
import json
import os
import tempfile
from unittest.mock import patch, MagicMock
import pytest
from wlm.recent_changes import LookupSet, RecentChangesClient


MONUMENTS_CSV = """\
country,lang,id,adm0,adm2,name,address,municipality,lat,lon,image,commonscat,source,changed,monument_article,wd_item,gallery,registrant_id,type,year_of_construction
ua,uk,80-361-0001,ua,,Київ,,Київ,,,img.jpg,Church Cat,,,Вознесенська церква,Q1,Church Gallery,,А,1889
ua,uk,14-101-0001,ua,,Дніпро,,Дніпро,,,,,,,,Монумент Слави,Q2,,,А,1967
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


# RecentChangesClient tests


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
    """fetch_with_token() returns the last rccontinue token seen."""
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
