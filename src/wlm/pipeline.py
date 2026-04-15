# src/wlm/pipeline.py
from dataclasses import dataclass
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from wlm.common import AdmLevel, KatotthKoatuuRepo, Lang, PopulatedPlaceRepo
from wlm.images import WlmSchema, cumulative_agg, transform, windowed_agg
from wlm.monuments import MonumentRepo


@dataclass
class MonumentPaths:
    # data/raw/ is produced by scripts/fetch_monuments.py and scripts/convert_humdata.py
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
    gold_by_adm = reduce(
        DataFrame.union,
        (
            repo.number_of_monuments_by_adm(level).select(
                F.lit(level.value).alias("level"),
                F.col("adm.code").alias("code"),
                F.col("adm.name").alias("name"),
                F.col("count").alias("monument_count"),
            )
            for level in adm_levels
        ),
    )
    _write(gold_by_adm, paths.gold_by_adm_dir, fmt)

    # Gold: pictured percentage per adm level (ADM1–ADM4 unioned)
    gold_pictured = reduce(
        DataFrame.union,
        (
            repo.percentage_of_pictured_monuments_by_adm(level).select(
                F.lit(level.value).alias("level"),
                F.col("adm.code").alias("code"),
                F.col("adm.name").alias("name"),
                F.col("all"),
                F.col("part"),
                F.col("percentage"),
            )
            for level in adm_levels
        ),
    )
    _write(gold_pictured, paths.gold_pictured_by_adm_dir, fmt)


def run_images_pipeline(
    spark: SparkSession,
    paths: ImagePaths | None = None,
    fmt: str = "parquet",
    lang: Lang = Lang.EN,
    window_duration: str = "365 days",  # "1 year" unsupported by F.window(); use fixed-day equivalent
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
