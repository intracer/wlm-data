import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


class WlmSchema:
    csv_schema = StructType([
        StructField("title", StringType()),
        StructField("author", StringType()),
        StructField("upload_date", StringType()),
        StructField("monument_id", StringType()),
        StructField("page_id", StringType()),
        StructField("width", StringType()),
        StructField("height", StringType()),
        StructField("size_bytes", StringType()),
        StructField("mime", StringType()),
        StructField("camera", StringType()),
        StructField("exif_date", StringType()),
        StructField("categories", StringType()),
        StructField("special_nominations", StringType()),
        StructField("url", StringType()),
        StructField("page_url", StringType()),
    ])

    transformed_schema = StructType([
        StructField("author", StringType(), nullable=True),
        StructField("monument", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("upload_date_ts", TimestampType(), nullable=True),
    ])


def transform(df: DataFrame) -> DataFrame:
    """
    Transforms a raw WLM CSV DataFrame into one row per monument-image pair.
    Mirrors Transformations.transform in the Scala codebase.
    Steps:
      1. Parse upload_date (ISO-8601) → upload_date_ts (TimestampType). Nulls tolerated.
      2. Split monument_id on ";" and explode → one row per monument. Empty/null dropped.
      3. Extract region as the first digits of the monument id (e.g. "14" from "14-101-0001").
      4. Select author, monument, region, upload_date_ts.
    """
    return (df
            .withColumn("upload_date_ts", F.coalesce(
                F.try_to_timestamp(F.col("upload_date"), F.lit("yyyy-MM-dd'T'HH:mm:ssXXX")),
                F.try_to_timestamp(F.col("upload_date"), F.lit("yyyy-MM-dd'T'HH:mmXXX")),
                F.try_to_timestamp(F.col("upload_date"), F.lit("yyyy-MM-dd")),
            ))
            .withColumn("monument", F.explode(F.split(F.col("monument_id"), ";")))
            .filter(F.col("monument") != "")
            .withColumn("region", F.regexp_extract(F.col("monument"), r"^(\d+)", 1))
            .select("author", "monument", "region", "upload_date_ts"))


def cumulative_agg(df: DataFrame, adm_names_df: DataFrame) -> DataFrame:
    """
    Cumulative aggregation: approximate distinct monuments per (author, region).
    Mirrors Queries.cumulativeAgg in the Scala codebase.
    """
    return (df
            .groupBy("author", "region")
            .agg(F.approx_count_distinct("monument").alias("monuments_pictured"))
            .join(
                adm_names_df,
                F.concat(F.lit("UA"), F.col("region")) == F.col("code"),
                "left"
            )
            .drop("code", "region")
            .withColumnRenamed("name", "region_name")
            .select("author", "region_name", "monuments_pictured")
            .sort(F.col("monuments_pictured").desc()))


def windowed_agg(
    df: DataFrame,
    adm_names_df: DataFrame,
    window_duration: str,
    watermark_duration: str,
) -> DataFrame:
    """
    Windowed aggregation: approximate distinct monuments per (window, author, region).
    Mirrors Queries.windowedAgg in the Scala codebase.
    """
    return (df
            .withWatermark("upload_date_ts", watermark_duration)
            .groupBy(
                F.window(F.col("upload_date_ts"), window_duration),
                F.col("author"),
                F.col("region")
            )
            .agg(F.approx_count_distinct("monument").alias("monuments_pictured"))
            .join(
                adm_names_df,
                F.concat(F.lit("UA"), F.col("region")) == F.col("code"),
                "left"
            )
            .drop("code", "region")
            .withColumnRenamed("name", "region_name")
            .select("window", "author", "region_name", "monuments_pictured"))
