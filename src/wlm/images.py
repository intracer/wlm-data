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
            .withColumn("upload_date_ts", F.to_timestamp(F.col("upload_date")))
            .withColumn("monument", F.explode(F.split(F.col("monument_id"), ";")))
            .filter(F.col("monument") != "")
            .withColumn("region", F.regexp_extract(F.col("monument"), r"^(\d+)", 1))
            .select("author", "monument", "region", "upload_date_ts"))
