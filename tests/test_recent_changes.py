import csv
import io
import pytest
from wlm.recent_changes import LookupSet


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
