import pytest
from pyspark.sql.types import StructType, StructField, StringType
from wlm.common import AdmLevel, Lang
from wlm.monuments import MonumentRepo

# ADM1 codes for all 27 Ukrainian regions (from PopulatedPlaceSpec)
ADM1_CODES = {
    "UA01", "UA05", "UA07", "UA12", "UA14", "UA18", "UA21", "UA23", "UA26",
    "UA32", "UA35", "UA44", "UA46", "UA48", "UA51", "UA53", "UA56", "UA59",
    "UA61", "UA63", "UA65", "UA68", "UA71", "UA73", "UA74", "UA80", "UA85",
}


def test_with_koatuu_from_id_standard_region(spark):
    """Standard region: id '14-101-0001' → adm1='UA14', adm2koatuu='1410100000'."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("municipality", StringType(), True),
        StructField("image", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("14-101-0001", "Monument", "Kyiv", None)],
        schema=schema
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA14"
    assert result[0].adm2koatuu == "1410100000"


def test_with_koatuu_from_id_kyiv_special_case(spark):
    """Kyiv (80): middle segment is always '000' regardless of id."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("municipality", StringType(), True),
        StructField("image", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("80-001-0001", "Monument", "Kyiv", None)],
        schema=schema
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA80"
    assert result[0].adm2koatuu == "8000000000"


def test_with_koatuu_from_id_sevastopol_special_case(spark):
    """Sevastopol (85): middle segment is always '000' regardless of id."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("municipality", StringType(), True),
        StructField("image", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("85-001-0001", "Monument", "Sevastopol", None)],
        schema=schema
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA85"
    assert result[0].adm2koatuu == "8500000000"
