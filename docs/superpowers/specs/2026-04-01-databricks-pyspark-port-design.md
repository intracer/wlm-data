# Databricks Free Edition PySpark Port — Design Spec

**Date:** 2026-04-01
**Branch:** databricks

## Overview

Port the existing Scala/Spark codebase to PySpark so it can run on Databricks Free Edition (Community Edition), which does not support Scala/JVM. Both the batch monuments pipeline and the structured streaming images pipeline are in scope. Unit tests are written first (TDD) and must run both locally and on Databricks.

---

## Project Structure

The repo gains a `src/` layout alongside Databricks notebooks. Business logic lives in importable Python modules; notebooks are thin wrappers. Tests import the same modules.

```
wlm-data/
├── src/wlm/
│   ├── __init__.py
│   ├── common.py          # AdmLevel, Lang enums; PopulatedPlaceRepo; KatotthKoatuuRepo;
│   │                      # clean_municipality_col expression
│   ├── monuments.py       # MonumentRepo
│   └── images.py          # Transformations.transform(); Queries.cumulative_agg();
│                          # Queries.windowed_agg()
├── tests/
│   ├── conftest.py        # local SparkSession fixture
│   ├── test_monuments.py  # tests for MonumentRepo methods
│   └── test_images.py     # tests for Transformations and Queries
├── notebooks/
│   ├── monuments.py       # Databricks notebook: runs monuments batch pipeline
│   └── images_streaming.py # Databricks notebook: runs streaming pipeline
├── requirements.txt       # pyspark==3.5.*, pytest
└── build.sbt              # unchanged — existing Scala code stays
```

The existing Scala source (`src/main/scala/`, `src/test/scala/`) is left in place. The Python source lives alongside it under `src/wlm/`.

---

## Data Layout on DBFS

Files uploaded manually via Databricks UI (`Data > Add Data > DBFS`):

**Input:**
```
dbfs:/FileStore/wlm-data/monuments/wlm-ua-monuments.csv
dbfs:/FileStore/wlm-data/humdata/ukraine-populated-places.csv
dbfs:/FileStore/wlm-data/katotth/katotth_koatuu.csv
dbfs:/FileStore/wlm-data/images/          ← drop CSV files here for streaming input
```

**Output** (written by pipelines):
```
dbfs:/FileStore/wlm-data/output/monuments-with-cities/   ← parquet
dbfs:/FileStore/wlm-data/output/windowed/                ← parquet
dbfs:/FileStore/wlm-data/output/cumulative/              ← parquet
```

**Streaming checkpoints:**
```
dbfs:/FileStore/wlm-data/checkpoints/windowed/
dbfs:/FileStore/wlm-data/checkpoints/cumulative/
```

All paths are defined as constants at the top of each notebook.

---

## Module Design

### `src/wlm/common.py`

**Enums:**
```python
from enum import Enum

class AdmLevel(Enum):
    ADM1 = "ADM1"; ADM2 = "ADM2"; ADM3 = "ADM3"; ADM4 = "ADM4"

class Lang(Enum):
    EN = "EN"; UK = "UK"
```
Mirror the Scala `AdmLevel` and `Lang` enumerations exactly.

**`clean_municipality_col(col)`** — pure PySpark column expression (no UDF). Chains `F.regexp_replace` and `F.trim` calls to replicate the Scala `Monument.cleanMunicipality` string replacements. Accepts a Column argument and returns a Column. No serialization overhead.

**`PopulatedPlaceRepo(spark, lang)`** — reads `ukraine-populated-places.csv`. Methods:
- `populated_places_df() -> DataFrame`
- `adm_names(adm_level: AdmLevel) -> DataFrame` — returns distinct `(code, name)` pairs for the given level

**`KatotthKoatuuRepo(spark)`** — reads `katotth_koatuu.csv`. Methods:
- `dataframe() -> DataFrame`
- `grouped_by_adm2_and_name() -> DataFrame`
- `unique_name_by_adm2() -> DataFrame`
- `non_unique_name_by_adm2() -> DataFrame`

### `src/wlm/monuments.py`

**`MonumentRepo(spark, lang)`** — replicates all methods from the Scala `MonumentRepo`:
- `dataframe() -> DataFrame`
- `with_koatuu_from_id() -> DataFrame`
- `cleaned_municipality_dataset() -> DataFrame` — applies `clean_municipality_col`
- `monuments_with_unmapped_koatuu() -> DataFrame`
- `joined_with_katotth() -> DataFrame`
- `group_by_adm(df, adm_level) -> DataFrame`
- `number_of_monuments_by_adm(adm_level) -> DataFrame`
- `number_of_pictured_monuments_by_adm(adm_level) -> DataFrame`
- `percentage_of_pictured_monuments_by_adm(adm_level) -> DataFrame`

No `Dataset[Monument]` — all methods return plain `DataFrame`. The `Monument`, `CountPerAdm`, `PercentagePerAdm` case class fields remain as DataFrame columns.

### `src/wlm/images.py`

**`transform(df) -> DataFrame`** — replicates `Transformations.transform`: parses `upload_date` → timestamp, explodes `monument_id` on `";"`, extracts `region` via regex, selects `(author, monument, region, upload_date_ts)`.

**`cumulative_agg(df, adm_names_df) -> DataFrame`** — replicates `Queries.cumulativeAgg`.

**`windowed_agg(df, adm_names_df, window_duration, watermark_duration) -> DataFrame`** — replicates `Queries.windowedAgg`.

---

## Notebooks

### `notebooks/monuments.py`

```
# Databricks notebook source
import sys; sys.path.insert(0, "src")

from wlm.common import Lang, AdmLevel
from wlm.monuments import MonumentRepo

# Config
MONUMENTS_CSV = "dbfs:/FileStore/wlm-data/monuments/wlm-ua-monuments.csv"
OUTPUT_DIR    = "dbfs:/FileStore/wlm-data/output/monuments-with-cities"

# spark is pre-created by Databricks
repo = MonumentRepo(spark, Lang.EN)
repo.joined_with_katotth().write.mode("overwrite").parquet(OUTPUT_DIR)
repo.percentage_of_pictured_monuments_by_adm(AdmLevel.ADM1).show(30, truncate=False)
```

### `notebooks/images_streaming.py`

```
# Databricks notebook source
import sys; sys.path.insert(0, "src")

from wlm.common import Lang, AdmLevel
from wlm.common import PopulatedPlaceRepo
from wlm.images import transform, windowed_agg

# Config — edit these as needed
INPUT_DIR      = "dbfs:/FileStore/wlm-data/images"
OUTPUT_DIR     = "dbfs:/FileStore/wlm-data/output"
CHECKPOINT_DIR = "dbfs:/FileStore/wlm-data/checkpoints"
WINDOW_DUR     = "1 hour"
WATERMARK_DUR  = "10 minutes"

adm_names_df = PopulatedPlaceRepo(spark, Lang.EN).adm_names(AdmLevel.ADM1)

raw_stream = spark.readStream.option("header", "true").csv(INPUT_DIR)
transformed = transform(raw_stream)

# trigger(availableNow=True): processes all current files then terminates
(windowed_agg(transformed, adm_names_df, WINDOW_DUR, WATERMARK_DUR)
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", f"{CHECKPOINT_DIR}/windowed")
  .trigger(availableNow=True)
  .foreachBatch(lambda df, _: df.sort("monuments_pictured", ascending=False)
                                 .write.mode("append").parquet(f"{OUTPUT_DIR}/windowed"))
  .start()
  .awaitTermination())
```

---

## Testing

### Strategy

TDD: write tests before implementation. Tests use `pyspark.testing.assertDataFrameEqual` (built into PySpark 3.5, no extra dependencies).

**Run locally:**
```bash
pip install pyspark==3.5.* pytest
pytest tests/
```

**Run on Databricks:**
A test notebook runs:
```python
%pip install pytest
import pytest, sys
sys.exit(pytest.main(["../tests", "-v"]))
```

### `tests/conftest.py`

Creates a local `SparkSession` as a session-scoped pytest fixture — mirrors the existing Scala test setup (spark-testing-base also creates a local session).

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local")
            .appName("wlm-tests")
            .getOrCreate())
```

### `tests/test_monuments.py`

Tests for each `MonumentRepo` method using small in-memory DataFrames:
- `test_clean_municipality_col` — verifies each replacement rule in `clean_municipality_col`
- `test_with_koatuu_from_id` — spot-checks KOATUU derivation and Kyiv/Sevastopol special cases
- `test_percentage_of_pictured_by_adm` — verifies percentage calculation with known data

### `tests/test_images.py`

- `test_transform` — verifies timestamp parsing, monument_id splitting on `";"`, region extraction
- `test_cumulative_agg` — verifies distinct monument count per author/region
- `test_windowed_agg` — verifies windowed grouping with a small static DataFrame (no real stream needed; PySpark allows testing stream transformations against static data)

---

## Key Translation Decisions

| Scala | PySpark |
|---|---|
| `Dataset[T]` / `.as[Monument]` | Plain `DataFrame` — no typed encoding |
| `import spark.implicits._` | Not needed |
| `object AdmLevel extends Enumeration` | `class AdmLevel(Enum)` |
| `Monument.cleanMunicipality` (method) | `clean_municipality_col(col)` (column expression, no UDF) |
| `ConfigFactory.load()` for streaming config | Python variables at notebook top |
| `SparkSession.builder()` | Databricks pre-created `spark` global |
| `awaitAnyTermination()` | `trigger(availableNow=True)` + `.awaitTermination()` |
| `spark-testing-base` / ScalaTest | `pytest` + `pyspark.testing.assertDataFrameEqual` |
| `foreachBatch { (df, _) => ... }` | `foreachBatch(lambda df, _: ...)` |

---

## Out of Scope

- Porting the existing Scala tests — the Scala codebase remains untouched
- Unity Catalog, external cloud storage
- Databricks Jobs / orchestration (run notebooks manually)
- Auto Loader (paid feature) — plain `readStream.csv` is used instead
