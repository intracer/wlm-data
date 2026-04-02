from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pytest
from wlm.images import WlmSchema, transform


INPUT_SCHEMA = StructType([
    StructField("author", StringType()),
    StructField("upload_date", StringType()),
    StructField("monument_id", StringType()),
])


def test_transform_splits_monument_id_on_semicolon(spark):
    df = spark.createDataFrame(
        [Row(author="Alice", upload_date="2022-10-01T10:00:00Z", monument_id="14-101-0001;14-101-0002")],
        INPUT_SCHEMA
    )
    result = transform(df)
    assert result.count() == 2
    monuments = {r.monument for r in result.collect()}
    assert monuments == {"14-101-0001", "14-101-0002"}


def test_transform_extracts_region(spark):
    df = spark.createDataFrame(
        [Row(author="Alice", upload_date="2022-10-01T10:00:00Z", monument_id="14-101-0001")],
        INPUT_SCHEMA
    )
    result = transform(df)
    assert result.collect()[0].region == "14"


def test_transform_null_upload_date_keeps_row_with_null_timestamp(spark):
    df = spark.createDataFrame(
        [Row(author="Alice", upload_date=None, monument_id="14-101-0001")],
        INPUT_SCHEMA
    )
    result = transform(df)
    assert result.count() == 1
    assert result.collect()[0].upload_date_ts is None


def test_transform_empty_or_null_monument_id_produces_no_rows(spark):
    df = spark.createDataFrame(
        [Row(author="Alice", upload_date="2022-10-01T10:00:00Z", monument_id=""),
         Row(author="Alice", upload_date="2022-10-01T10:00:00Z", monument_id=None)],
        INPUT_SCHEMA
    )
    result = transform(df)
    assert result.count() == 0


def test_transform_output_schema_matches_wlm_schema(spark):
    df = spark.createDataFrame(
        [Row(author="Alice", upload_date="2022-10-01T10:00:00Z", monument_id="14-101-0001")],
        INPUT_SCHEMA
    )
    result = transform(df)
    assert list(result.schema.fieldNames()) == ["author", "monument", "region", "upload_date_ts"]
    assert result.schema == WlmSchema.transformed_schema
