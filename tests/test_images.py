import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pytest
from wlm.images import WlmSchema, transform, cumulative_agg, windowed_agg


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


ADM_NAMES_SCHEMA = StructType([
    StructField("code", StringType()),
    StructField("name", StringType()),
])


@pytest.fixture
def empty_adm_names(spark):
    return spark.createDataFrame([], ADM_NAMES_SCHEMA)


def test_cumulative_agg_counts_distinct_monuments_per_author_region(spark, empty_adm_names):
    rows = [
        ("Alice", "14-101-0001", "14", None),
        ("Alice", "14-101-0002", "14", None),
        ("Alice", "14-101-0001", "14", None),   # duplicate — not counted twice
        ("Bob",   "14-102-0001", "14", None),
    ]
    df = spark.createDataFrame(rows, WlmSchema.transformed_schema)
    result = cumulative_agg(df, empty_adm_names)
    alice = [r for r in result.collect() if r.author == "Alice"]
    assert len(alice) == 1
    assert alice[0].monuments_pictured == 2
    bob = [r for r in result.collect() if r.author == "Bob"]
    assert len(bob) == 1
    assert bob[0].monuments_pictured == 1


def test_cumulative_agg_produces_separate_row_per_author_region_pair(spark, empty_adm_names):
    rows = [
        ("Alice", "14-101-0001", "14", None),
        ("Alice", "15-101-0001", "15", None),
    ]
    df = spark.createDataFrame(rows, WlmSchema.transformed_schema)
    result = cumulative_agg(df, empty_adm_names)
    assert result.count() == 2


def test_windowed_agg_smoke_streaming(spark, empty_adm_names, tmp_path):
    """Windowed agg streaming query starts and terminates without errors."""
    input_dir = tmp_path / "images"
    input_dir.mkdir()
    checkpoint_dir = tmp_path / "checkpoint"
    output_dir = tmp_path / "output"

    with open(input_dir / "batch1.csv", "w") as f:
        f.write("author,monument,region,upload_date_ts\n")
        f.write("Alice,14-101-0001,14,2022-10-01 10:00:00\n")
        f.write("Alice,14-101-0002,14,2022-10-01 10:05:00\n")

    raw_stream = (spark.readStream
                  .schema(WlmSchema.transformed_schema)
                  .option("header", "true")
                  .csv(str(input_dir)))

    query = (windowed_agg(raw_stream, empty_adm_names, "1 hour", "10 minutes")
             .writeStream
             .outputMode("append")
             .option("checkpointLocation", str(checkpoint_dir))
             .trigger(availableNow=True)
             .format("memory")
             .queryName("windowed_smoke_test")
             .start())
    query.awaitTermination(timeout=60)
    assert query.exception() is None
