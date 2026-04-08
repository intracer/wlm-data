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
