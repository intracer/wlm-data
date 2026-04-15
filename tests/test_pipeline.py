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
