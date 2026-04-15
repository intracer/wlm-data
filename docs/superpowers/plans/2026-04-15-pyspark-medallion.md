# PySpark Medallion Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bronze/silver/gold medallion layers to the PySpark pipeline (monuments + images) and wire it to consume data from the shared fetch scripts in `data/raw/`.

**Architecture:** A new `src/wlm/pipeline.py` module provides `run_monuments_pipeline` and `run_images_pipeline` functions that accept path dataclasses and a format (`"parquet"` or `"delta"`). Existing repo classes in `common.py` and `monuments.py` are left unchanged; the pipeline functions call them and write each layer. Entry points (`run_monuments.py`, Databricks notebooks, Dagster asset) are updated to call the new functions.

**Tech Stack:** PySpark (local mode for tests), Delta Lake (Databricks), Dagster, pytest, Python dataclasses

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `src/wlm/pipeline.py` | Create | `MonumentPaths`, `ImagePaths`, `run_monuments_pipeline`, `run_images_pipeline` |
| `tests/test_pipeline.py` | Create | Smoke tests for both pipeline functions |
| `run_monuments.py` | Modify | Replace direct repo calls with `run_monuments_pipeline` using local paths |
| `notebooks/monuments.py` | Modify | Replace direct repo calls with `run_monuments_pipeline` using DBFS/Delta paths |
| `notebooks/images_streaming.py` | Modify | Replace streaming logic with `run_images_pipeline` using DBFS/Delta paths |
| `orchestration/definitions.py` | Modify | `spark_output` asset calls pipeline functions; `compare_outputs` reads from gold layer path |

---

## Task 1: Write failing tests for `pipeline.py`

**Files:**
- Create: `tests/test_pipeline.py`

- [ ] **Step 1: Create the test file**

```python
# tests/test_pipeline.py
import glob as glob_mod
import pytest
from wlm.pipeline import ImagePaths, MonumentPaths, run_images_pipeline, run_monuments_pipeline


def test_run_monuments_pipeline_writes_all_layers(spark, tmp_path):
    paths = MonumentPaths(
        monuments_csv="data/wiki/monuments/wlm-ua-monuments.csv",
        humdata_csv="data/humdata/ukraine-populated-places.csv",
        katotth_csv="data/katotth/katotth_koatuu.csv",
        bronze_dir=str(tmp_path / "monuments/bronze"),
        silver_cleaned_dir=str(tmp_path / "monuments/silver/cleaned"),
        silver_with_cities_dir=str(tmp_path / "monuments/silver/with_cities"),
        gold_full_dir=str(tmp_path / "monuments/gold/full"),
        gold_by_adm_dir=str(tmp_path / "monuments/gold/by_adm"),
        gold_pictured_by_adm_dir=str(tmp_path / "monuments/gold/pictured_by_adm"),
    )
    run_monuments_pipeline(spark, paths, fmt="parquet")

    for layer_dir in [
        paths.bronze_dir,
        paths.silver_cleaned_dir,
        paths.silver_with_cities_dir,
        paths.gold_full_dir,
        paths.gold_by_adm_dir,
        paths.gold_pictured_by_adm_dir,
    ]:
        parquet_files = glob_mod.glob(f"{layer_dir}/*.parquet")
        assert len(parquet_files) > 0, f"No parquet files written to {layer_dir}"


def test_run_monuments_pipeline_gold_full_has_adm_name_columns(spark, tmp_path):
    paths = MonumentPaths(
        monuments_csv="data/wiki/monuments/wlm-ua-monuments.csv",
        humdata_csv="data/humdata/ukraine-populated-places.csv",
        katotth_csv="data/katotth/katotth_koatuu.csv",
        bronze_dir=str(tmp_path / "monuments/bronze"),
        silver_cleaned_dir=str(tmp_path / "monuments/silver/cleaned"),
        silver_with_cities_dir=str(tmp_path / "monuments/silver/with_cities"),
        gold_full_dir=str(tmp_path / "monuments/gold/full"),
        gold_by_adm_dir=str(tmp_path / "monuments/gold/by_adm"),
        gold_pictured_by_adm_dir=str(tmp_path / "monuments/gold/pictured_by_adm"),
    )
    run_monuments_pipeline(spark, paths, fmt="parquet")

    gold = spark.read.parquet(paths.gold_full_dir)
    cols = set(gold.columns)
    assert {"id", "name", "municipality", "image",
            "adm1", "adm2", "adm3", "adm4",
            "adm1_name", "adm2_name", "adm3_name"}.issubset(cols)
    assert gold.count() > 0


def test_run_monuments_pipeline_gold_by_adm_has_all_levels(spark, tmp_path):
    paths = MonumentPaths(
        monuments_csv="data/wiki/monuments/wlm-ua-monuments.csv",
        humdata_csv="data/humdata/ukraine-populated-places.csv",
        katotth_csv="data/katotth/katotth_koatuu.csv",
        bronze_dir=str(tmp_path / "monuments/bronze"),
        silver_cleaned_dir=str(tmp_path / "monuments/silver/cleaned"),
        silver_with_cities_dir=str(tmp_path / "monuments/silver/with_cities"),
        gold_full_dir=str(tmp_path / "monuments/gold/full"),
        gold_by_adm_dir=str(tmp_path / "monuments/gold/by_adm"),
        gold_pictured_by_adm_dir=str(tmp_path / "monuments/gold/pictured_by_adm"),
    )
    run_monuments_pipeline(spark, paths, fmt="parquet")

    gold = spark.read.parquet(paths.gold_by_adm_dir)
    levels = {r.level for r in gold.select("level").distinct().collect()}
    assert levels == {"ADM1", "ADM2", "ADM3", "ADM4"}
    cols = set(gold.columns)
    assert {"level", "code", "name", "monument_count"}.issubset(cols)


def test_run_images_pipeline_writes_all_layers(spark, tmp_path):
    paths = ImagePaths(
        images_dir="data/images",
        humdata_csv="data/humdata/ukraine-populated-places.csv",
        bronze_dir=str(tmp_path / "images/bronze"),
        silver_dir=str(tmp_path / "images/silver"),
        gold_cumulative_dir=str(tmp_path / "images/gold/cumulative"),
        gold_windowed_dir=str(tmp_path / "images/gold/windowed"),
    )
    run_images_pipeline(spark, paths, fmt="parquet")

    for layer_dir in [
        paths.bronze_dir,
        paths.silver_dir,
        paths.gold_cumulative_dir,
        paths.gold_windowed_dir,
    ]:
        parquet_files = glob_mod.glob(f"{layer_dir}/*.parquet")
        assert len(parquet_files) > 0, f"No parquet files written to {layer_dir}"
```

- [ ] **Step 2: Run to confirm they all fail (ImportError expected)**

```bash
.venv/bin/pytest tests/test_pipeline.py -v 2>&1 | head -30
```

Expected: `ImportError: cannot import name 'MonumentPaths' from 'wlm.pipeline'` (or `ModuleNotFoundError`)

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_pipeline.py
git commit -m "test: add failing smoke tests for monument and image pipeline layers"
```

---

## Task 2: Create `src/wlm/pipeline.py`

**Files:**
- Create: `src/wlm/pipeline.py`

- [ ] **Step 1: Create the module**

```python
# src/wlm/pipeline.py
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from wlm.common import AdmLevel, KatotthKoatuuRepo, Lang, PopulatedPlaceRepo
from wlm.images import WlmSchema, cumulative_agg, transform, windowed_agg
from wlm.monuments import MonumentRepo


@dataclass
class MonumentPaths:
    monuments_csv: str = "data/raw/monuments.csv"
    humdata_csv: str = "data/raw/humdata.csv"
    katotth_csv: str = KatotthKoatuuRepo.DEFAULT_PATH
    bronze_dir: str = "data/processed/spark/monuments/bronze"
    silver_cleaned_dir: str = "data/processed/spark/monuments/silver/cleaned"
    silver_with_cities_dir: str = "data/processed/spark/monuments/silver/with_cities"
    gold_full_dir: str = "data/processed/spark/monuments/gold/full"
    gold_by_adm_dir: str = "data/processed/spark/monuments/gold/by_adm"
    gold_pictured_by_adm_dir: str = "data/processed/spark/monuments/gold/pictured_by_adm"


@dataclass
class ImagePaths:
    images_dir: str = "data/images"
    humdata_csv: str = "data/raw/humdata.csv"
    bronze_dir: str = "data/processed/spark/images/bronze"
    silver_dir: str = "data/processed/spark/images/silver"
    gold_cumulative_dir: str = "data/processed/spark/images/gold/cumulative"
    gold_windowed_dir: str = "data/processed/spark/images/gold/windowed"


def _write(df: DataFrame, path: str, fmt: str) -> None:
    df.write.mode("overwrite").format(fmt).save(path)


def run_monuments_pipeline(
    spark: SparkSession,
    paths: MonumentPaths | None = None,
    fmt: str = "parquet",
    lang: Lang = Lang.EN,
) -> None:
    if paths is None:
        paths = MonumentPaths()

    repo = MonumentRepo(
        spark, lang,
        path=paths.monuments_csv,
        humdata_path=paths.humdata_csv,
        katotth_path=paths.katotth_csv,
    )

    # Bronze: raw CSV as-is
    bronze = repo.dataframe()
    _write(bronze, paths.bronze_dir, fmt)

    # Silver: cleaned (derived adm codes, normalised municipality)
    silver_cleaned = repo.cleaned_municipality_dataset()
    _write(silver_cleaned, paths.silver_cleaned_dir, fmt)

    # Silver: with cities (fully resolved adm1–adm4 + municipality name)
    silver_with_cities = repo.joined_with_katotth()
    _write(silver_with_cities, paths.silver_with_cities_dir, fmt)

    # Gold: full (add human-readable adm names)
    humdata = PopulatedPlaceRepo(spark, lang, path=paths.humdata_csv)
    adm1_names = humdata.adm_names(AdmLevel.ADM1).select(
        F.col("code").alias("adm1_code"), F.col("name").alias("adm1_name")
    )
    adm2_names = humdata.adm_names(AdmLevel.ADM2).select(
        F.col("code").alias("adm2_code"), F.col("name").alias("adm2_name")
    )
    adm3_names = humdata.adm_names(AdmLevel.ADM3).select(
        F.col("code").alias("adm3_code"), F.col("name").alias("adm3_name")
    )
    gold_full = (
        silver_with_cities
        .join(adm1_names, F.col("adm1") == F.col("adm1_code"), "left").drop("adm1_code")
        .join(adm2_names, F.col("adm2") == F.col("adm2_code"), "left").drop("adm2_code")
        .join(adm3_names, F.col("adm3") == F.col("adm3_code"), "left").drop("adm3_code")
    )
    _write(gold_full, paths.gold_full_dir, fmt)

    # Gold: monument count per adm level (ADM1–ADM4 unioned, level column added)
    adm_levels = [AdmLevel.ADM1, AdmLevel.ADM2, AdmLevel.ADM3, AdmLevel.ADM4]
    gold_by_adm: DataFrame | None = None
    for level in adm_levels:
        df = repo.number_of_monuments_by_adm(level).select(
            F.lit(level.value).alias("level"),
            F.col("adm.code").alias("code"),
            F.col("adm.name").alias("name"),
            F.col("count").alias("monument_count"),
        )
        gold_by_adm = df if gold_by_adm is None else gold_by_adm.union(df)
    _write(gold_by_adm, paths.gold_by_adm_dir, fmt)

    # Gold: pictured percentage per adm level (ADM1–ADM4 unioned)
    gold_pictured: DataFrame | None = None
    for level in adm_levels:
        df = repo.percentage_of_pictured_monuments_by_adm(level).select(
            F.lit(level.value).alias("level"),
            F.col("adm.code").alias("code"),
            F.col("adm.name").alias("name"),
            F.col("all"),
            F.col("part"),
            F.col("percentage"),
        )
        gold_pictured = df if gold_pictured is None else gold_pictured.union(df)
    _write(gold_pictured, paths.gold_pictured_by_adm_dir, fmt)


def run_images_pipeline(
    spark: SparkSession,
    paths: ImagePaths | None = None,
    fmt: str = "parquet",
    lang: Lang = Lang.EN,
    window_duration: str = "1 year",
    watermark_duration: str = "30 days",
) -> None:
    if paths is None:
        paths = ImagePaths()

    # Bronze: raw CSV files as-is
    bronze = (spark.read
              .schema(WlmSchema.csv_schema)
              .option("header", "true")
              .csv(paths.images_dir))
    _write(bronze, paths.bronze_dir, fmt)

    # Silver: exploded monument IDs, parsed timestamps
    silver = transform(bronze)
    _write(silver, paths.silver_dir, fmt)

    # Gold: cumulative monuments pictured per author+region
    humdata = PopulatedPlaceRepo(spark, lang, path=paths.humdata_csv)
    adm_names = humdata.adm_names(AdmLevel.ADM1)
    gold_cumulative = cumulative_agg(silver, adm_names)
    _write(gold_cumulative, paths.gold_cumulative_dir, fmt)

    # Gold: windowed monuments pictured per author+region+window
    gold_windowed = windowed_agg(silver, adm_names, window_duration, watermark_duration)
    _write(gold_windowed, paths.gold_windowed_dir, fmt)
```

- [ ] **Step 2: Run the tests**

```bash
.venv/bin/pytest tests/test_pipeline.py -v
```

Expected: all 4 tests pass. If any fail, check the error — likely a column name mismatch or a missing import.

- [ ] **Step 3: Run the full test suite to check for regressions**

```bash
.venv/bin/pytest tests/ -v
```

Expected: all tests pass (pipeline tests are new; existing tests are unaffected since we didn't touch `common.py` or `monuments.py`).

- [ ] **Step 4: Commit**

```bash
git add src/wlm/pipeline.py
git commit -m "feat: add pipeline.py with MonumentPaths, ImagePaths, run_monuments_pipeline, run_images_pipeline"
```

---

## Task 3: Update `run_monuments.py`

**Files:**
- Modify: `run_monuments.py`

Current file creates a `MonumentRepo` directly and calls methods on it. Replace it with a call to `run_monuments_pipeline`.

- [ ] **Step 1: Rewrite `run_monuments.py`**

```python
# run_monuments.py
import sys
sys.path.insert(0, "src")

from pyspark.sql import SparkSession

from wlm.pipeline import MonumentPaths, run_monuments_pipeline

spark = (SparkSession.builder
         .master("local[*]")
         .appName("wlm-monuments")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

paths = MonumentPaths(
    monuments_csv="data/raw/monuments.csv",
    humdata_csv="data/raw/humdata.csv",
    katotth_csv="data/katotth/katotth_koatuu.csv",
)

run_monuments_pipeline(spark, paths, fmt="parquet")
print("Written to data/processed/spark/monuments/")

spark.stop()
```

- [ ] **Step 2: Verify the script is importable (no syntax errors)**

```bash
.venv/bin/python -c "import run_monuments" 2>&1 | head -5
```

Expected: no output (or only Spark startup warnings).

- [ ] **Step 3: Commit**

```bash
git add run_monuments.py
git commit -m "feat: update run_monuments.py to use run_monuments_pipeline with data/raw/ inputs"
```

---

## Task 4: Update `notebooks/monuments.py`

**Files:**
- Modify: `notebooks/monuments.py`

Replace direct `MonumentRepo` calls with `run_monuments_pipeline`. All paths stay as DBFS Volume paths. Format changes to `"delta"`.

- [ ] **Step 1: Rewrite the notebook**

```python
# Databricks notebook source
# This notebook runs the monuments batch pipeline (bronze → silver → gold).
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Raw CSV files in the DBFS paths below (run fetch/convert scripts first)

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

# COMMAND ----------

from wlm.pipeline import MonumentPaths, run_monuments_pipeline

# COMMAND ----------
# Config — edit DBFS paths as needed

BASE = "dbfs:/Volumes/workspace/default/wlm_data"

paths = MonumentPaths(
    monuments_csv=f"{BASE}/raw/monuments.csv",
    humdata_csv=f"{BASE}/raw/humdata.csv",
    katotth_csv=f"{BASE}/katotth/katotth_koatuu.csv",
    bronze_dir=f"{BASE}/bronze/monuments",
    silver_cleaned_dir=f"{BASE}/silver/monuments_cleaned",
    silver_with_cities_dir=f"{BASE}/silver/monuments_with_cities",
    gold_full_dir=f"{BASE}/gold/monuments_full",
    gold_by_adm_dir=f"{BASE}/gold/monuments_by_adm",
    gold_pictured_by_adm_dir=f"{BASE}/gold/pictured_by_adm",
)

# COMMAND ----------
# spark is pre-created by Databricks — no SparkSession.builder needed

run_monuments_pipeline(spark, paths, fmt="delta")
print(f"Pipeline complete. Gold layer written to {BASE}/gold/")
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/monuments.py
git commit -m "feat: update monuments notebook to write bronze/silver/gold Delta layers on DBFS"
```

---

## Task 5: Update `notebooks/images_streaming.py`

**Files:**
- Modify: `notebooks/images_streaming.py`

Replace the manual streaming logic with `run_images_pipeline`. The `availableNow=True` trigger meant the streaming query was effectively batch anyway; `run_images_pipeline` processes all available files and terminates.

- [ ] **Step 1: Rewrite the notebook**

```python
# Databricks notebook source
# This notebook runs the images pipeline (bronze → silver → gold).
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Image CSV files in INPUT_DIR below

import sys
sys.path.insert(0, "src")

# COMMAND ----------

from wlm.pipeline import ImagePaths, run_images_pipeline

# COMMAND ----------
# Config — edit DBFS paths as needed

BASE = "dbfs:/Volumes/workspace/default/wlm_data"

paths = ImagePaths(
    images_dir=f"{BASE}/images",
    humdata_csv=f"{BASE}/raw/humdata.csv",
    bronze_dir=f"{BASE}/bronze/images",
    silver_dir=f"{BASE}/silver/images",
    gold_cumulative_dir=f"{BASE}/gold/images_cumulative",
    gold_windowed_dir=f"{BASE}/gold/images_windowed",
)

# COMMAND ----------
# spark is pre-created by Databricks

run_images_pipeline(
    spark,
    paths,
    fmt="delta",
    window_duration="1 year",
    watermark_duration="30 days",
)
print(f"Pipeline complete. Gold layer written to {BASE}/gold/")
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/images_streaming.py
git commit -m "feat: update images notebook to write bronze/silver/gold Delta layers on DBFS"
```

---

## Task 6: Update `orchestration/definitions.py`

**Files:**
- Modify: `orchestration/definitions.py`

Two changes:
1. `spark_output` asset: replace `sbt` subprocess call with direct Python calls to `run_monuments_pipeline` and `run_images_pipeline`
2. `compare_outputs` asset: update Spark parquet read path to `gold/full` layer

- [ ] **Step 1: Update imports and constants at the top of `orchestration/definitions.py`**

Replace the existing imports block and constants with:

```python
import subprocess
from pathlib import Path

import duckdb
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    AssetSelection,
)
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wlm.pipeline import ImagePaths, MonumentPaths, run_images_pipeline, run_monuments_pipeline

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
SPARK_OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "spark"
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "dbt"
COMPARISON_REPORT = REPO_ROOT / "data" / "processed" / "comparison_report.md"
```

- [ ] **Step 2: Replace the `spark_output` asset body**

Find the `spark_output` asset (currently calls `sbt`) and replace its body:

```python
@asset(deps=[monuments_raw, humdata_raw])
def spark_output(context: AssetExecutionContext):
    """Run PySpark pipeline — writes bronze/silver/gold parquet layers."""
    SPARK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("wlm-dagster")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    try:
        monument_paths = MonumentPaths(
            monuments_csv=str(RAW_DIR / "monuments.csv"),
            humdata_csv=str(RAW_DIR / "humdata.csv"),
            katotth_csv=str(REPO_ROOT / "data" / "katotth" / "katotth_koatuu.csv"),
            bronze_dir=str(SPARK_OUTPUT_DIR / "monuments" / "bronze"),
            silver_cleaned_dir=str(SPARK_OUTPUT_DIR / "monuments" / "silver" / "cleaned"),
            silver_with_cities_dir=str(SPARK_OUTPUT_DIR / "monuments" / "silver" / "with_cities"),
            gold_full_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"),
            gold_by_adm_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "by_adm"),
            gold_pictured_by_adm_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "pictured_by_adm"),
        )
        run_monuments_pipeline(spark, monument_paths, fmt="parquet")
        context.log.info("Monuments pipeline complete")

        image_paths = ImagePaths(
            images_dir=str(REPO_ROOT / "data" / "images"),
            humdata_csv=str(RAW_DIR / "humdata.csv"),
            bronze_dir=str(SPARK_OUTPUT_DIR / "images" / "bronze"),
            silver_dir=str(SPARK_OUTPUT_DIR / "images" / "silver"),
            gold_cumulative_dir=str(SPARK_OUTPUT_DIR / "images" / "gold" / "cumulative"),
            gold_windowed_dir=str(SPARK_OUTPUT_DIR / "images" / "gold" / "windowed"),
        )
        run_images_pipeline(spark, image_paths, fmt="parquet")
        context.log.info("Images pipeline complete")
    finally:
        spark.stop()

    gold_dir = SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"
    parquet_files = list(gold_dir.glob("*.parquet"))
    context.add_output_metadata({"gold_parquet_files": len(parquet_files), "gold_path": str(gold_dir)})
```

- [ ] **Step 3: Update `compare_outputs` to read from the gold layer path**

Find the `spark_df = conn.execute(...)` call inside `compare_outputs` and update the path:

```python
    gold_spark_dir = SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"
    conn = duckdb.connect()
    spark_df = conn.execute(
        f"SELECT id, adm1, adm2, adm3, adm4, municipality "
        f"FROM read_parquet('{gold_spark_dir}/*.parquet') ORDER BY id"
    ).df()
    conn.close()
```

(The rest of `compare_outputs` stays identical.)

- [ ] **Step 4: Run the full test suite to confirm nothing broke**

```bash
.venv/bin/pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add orchestration/definitions.py
git commit -m "feat: update spark_output Dagster asset to use run_monuments_pipeline and run_images_pipeline"
```
