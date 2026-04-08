import csv
import json
import os
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
            has_gallery = "gallery" in (reader.fieldnames or [])
            if not has_gallery:
                warnings.warn("monuments CSV has no 'gallery' column — skipping")
            for row in reader:
                self._set("uk.wikipedia.org", row.get("name", ""), "place_article")
                self._set("uk.wikipedia.org", row.get("monument_article", ""), "monument_article")
                self._set("commons.wikimedia.org", row.get("commonscat", ""), "commons_category")
                if has_gallery:
                    self._set("commons.wikimedia.org", row.get("gallery", ""), "commons_category")

    def _load_images(self, path: str) -> None:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self._set("commons.wikimedia.org", row.get("title", ""), "commons_file")

    def source_type(self, wiki: str, title: str) -> Optional[str]:
        return self._map.get((wiki, title))


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
                    record = dict(rc)  # Copy to avoid mutating the API response
                    record["wiki"] = wiki
                    all_records.append(record)

                if "continue" in data:
                    token = data["continue"]["rccontinue"]
                    last_token = token
                    params["rccontinue"] = token
                else:
                    break

        return all_records, last_token
