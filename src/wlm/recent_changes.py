import csv
import json
import os
import warnings
from typing import Optional
import requests
from pyspark.sql import SparkSession
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
                "rcdir": "newer",
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
