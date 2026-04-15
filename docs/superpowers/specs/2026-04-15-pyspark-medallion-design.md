# PySpark Medallion Architecture Design

**Date:** 2026-04-15  
**Branch:** feature/dbt-pipeline

## Overview

Update the PySpark/Databricks pipeline to:
1. Read raw data from the shared fetch layer (`data/raw/`) instead of ad-hoc CSV paths
2. Write outputs in bronze/silver/gold medallion layers, mirroring the dbt pipeline structure
3. Cover both the monuments pipeline and the images pipeline

## Architecture & Data Flow

```
scripts/fetch_monuments.py  →  data/raw/monuments.csv
scripts/convert_humdata.py  →  data/raw/humdata.csv
data/katotth/katotth_koatuu.csv  (static, unchanged)
         │
         ▼
src/wlm/pipeline.py
  run_monuments_pipeline(spark, paths, fmt, lang)
    Bronze  → raw CSV as-is
    Silver  → cleaned monuments, monuments joined with cities
    Gold    → monuments_full, monuments_by_adm, pictured_by_adm

  run_images_pipeline(spark, paths, fmt, window_duration, watermark_duration)
    Bronze  → raw CSV as-is
    Silver  → exploded/transformed images
    Gold    → cumulative_agg, windowed_agg
```

### Local output paths (parquet)

```
data/processed/spark/monuments/bronze/
data/processed/spark/monuments/silver/cleaned/
data/processed/spark/monuments/silver/with_cities/
data/processed/spark/monuments/gold/full/
data/processed/spark/monuments/gold/by_adm/
data/processed/spark/monuments/gold/pictured_by_adm/

data/processed/spark/images/bronze/
data/processed/spark/images/silver/
data/processed/spark/images/gold/cumulative/
data/processed/spark/images/gold/windowed/
```

### Databricks output paths (Delta Lake)

```
dbfs:/Volumes/workspace/default/wlm_data/bronze/monuments
dbfs:/Volumes/workspace/default/wlm_data/bronze/humdata
dbfs:/Volumes/workspace/default/wlm_data/bronze/katotth
dbfs:/Volumes/workspace/default/wlm_data/silver/monuments_cleaned
dbfs:/Volumes/workspace/default/wlm_data/silver/monuments_with_cities
dbfs:/Volumes/workspace/default/wlm_data/gold/monuments_full
dbfs:/Volumes/workspace/default/wlm_data/gold/monuments_by_adm
dbfs:/Volumes/workspace/default/wlm_data/gold/pictured_by_adm

dbfs:/Volumes/workspace/default/wlm_data/bronze/images
dbfs:/Volumes/workspace/default/wlm_data/silver/images
dbfs:/Volumes/workspace/default/wlm_data/gold/images_cumulative
dbfs:/Volumes/workspace/default/wlm_data/gold/images_windowed
```

## New Module: `src/wlm/pipeline.py`

### Dataclasses

```python
@dataclass
class MonumentPaths:
    monuments_csv: str       # input: data/raw/monuments.csv
    humdata_csv: str         # input: data/raw/humdata.csv
    katotth_csv: str         # input: data/katotth/katotth_koatuu.csv
    bronze_dir: str
    silver_cleaned_dir: str
    silver_with_cities_dir: str
    gold_full_dir: str
    gold_by_adm_dir: str
    gold_pictured_by_adm_dir: str

@dataclass
class ImagePaths:
    images_csv: str          # glob, e.g. data/images/wlm-UA-*.csv
    bronze_dir: str
    silver_dir: str
    gold_cumulative_dir: str
    gold_windowed_dir: str
```

### Functions

```python
def run_monuments_pipeline(
    spark: SparkSession,
    paths: MonumentPaths,
    fmt: str = "parquet",   # "delta" on Databricks
    lang: Lang = Lang.EN,
) -> None

def run_images_pipeline(
    spark: SparkSession,
    paths: ImagePaths,
    fmt: str = "parquet",
    window_duration: str = "1 year",
    watermark_duration: str = "30 days",
) -> None
```

Each function writes each layer with `mode("overwrite")`.

## Layer Contents

### Monuments

| Layer | Dataset | Source |
|-------|---------|--------|
| Bronze | raw monuments | CSV as-is |
| Silver | cleaned | `MonumentRepo.cleaned_municipality_dataset()` — derived adm codes, cleaned municipality |
| Silver | with_cities | `MonumentRepo.joined_with_katotth()` — fully resolved adm1–adm4 + municipality name |
| Gold | full | silver/with_cities joined with humdata adm names (mirrors `gold_monuments_full` in dbt) |
| Gold | by_adm | `MonumentRepo.number_of_monuments_by_adm()` for ADM1–ADM4 (unioned, with level column) |
| Gold | pictured_by_adm | `MonumentRepo.percentage_of_pictured_monuments_by_adm()` for ADM1–ADM4 |

### Images

| Layer | Dataset | Source |
|-------|---------|--------|
| Bronze | raw images | CSV as-is |
| Silver | transformed | `images.transform()` — exploded monument IDs, parsed timestamps |
| Gold | cumulative | `images.cumulative_agg()` — monuments pictured per author+region |
| Gold | windowed | `images.windowed_agg()` — same per time window |

## Changes to Existing Files

### `src/wlm/common.py`
- `PopulatedPlaceRepo.DEFAULT_PATH`: `data/humdata/ukraine-populated-places.csv` → `data/raw/humdata.csv`
- `MonumentRepo.DEFAULT_PATH`: `data/wiki/monuments/wlm-ua-monuments.csv` → `data/raw/monuments.csv`

### `run_monuments.py`
- Construct `MonumentPaths` with local `data/raw/` inputs and `data/processed/spark/` outputs
- Call `run_monuments_pipeline(spark, paths, fmt="parquet")`
- Remove direct repo calls (now encapsulated in pipeline)

### `notebooks/monuments.py` (Databricks)
- Construct `MonumentPaths` with DBFS input and output paths
- Call `run_monuments_pipeline(spark, paths, fmt="delta")`

### `notebooks/images_streaming.py` (Databricks)
- Construct `ImagePaths` with DBFS paths
- Call `run_images_pipeline(spark, paths, fmt="delta")`

### `orchestration/definitions.py`
- `spark_output` asset: call `run_monuments_pipeline` + `run_images_pipeline` with local paths and `fmt="parquet"`
- `compare_outputs` asset: update Spark read path from old flat output to `gold/full/` layer path

## Testing

Existing unit tests inject DataFrames directly into repo methods and are unaffected by this change. `pipeline.py` is a thin orchestration layer with no novel transformation logic — no new unit tests required. Integration behavior is validated by the existing `compare_outputs` Dagster asset.

## Non-Goals

- No changes to the dbt pipeline
- No changes to `KatotthKoatuuRepo.DEFAULT_PATH` (already points to `data/katotth/`, which is correct)
- No streaming changes to the images pipeline — windowed/cumulative aggregation interface stays the same
