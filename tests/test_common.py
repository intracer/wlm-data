from pyspark.sql.functions import col
from wlm.common import AdmLevel, Lang, clean_municipality_col


def test_adm_level_values():
    assert AdmLevel.ADM0.value == "ADM0"
    assert AdmLevel.ADM1.value == "ADM1"
    assert AdmLevel.ADM2.value == "ADM2"
    assert AdmLevel.ADM3.value == "ADM3"
    assert AdmLevel.ADM4.value == "ADM4"


def test_lang_values():
    assert Lang.EN.value == "EN"
    assert Lang.UK.value == "UK"


def test_clean_municipality_removes_wiki_link_brackets(spark):
    df = spark.createDataFrame([("[[Kyiv]]",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Kyiv"


def test_clean_municipality_removes_disambiguating_wiki_link(spark):
    # "[[Dnipro (city)|Dnipro]]" → "Dnipro"
    # The "(city)" part is stripped by the split-on-"(" rule.
    # The "|Dnipro" part is stripped by the split-on-"|" rule.
    df = spark.createDataFrame([("[[Dnipro (city)|Dnipro]]",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Dnipro"


def test_clean_municipality_removes_city_abbreviation(spark):
    df = spark.createDataFrame([("м. Kyiv",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Kyiv"


def test_clean_municipality_removes_village_abbreviation(spark):
    df = spark.createDataFrame([("с. Bohuslav",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Bohuslav"


def test_clean_municipality_removes_sel_abbreviation(spark):
    df = spark.createDataFrame([("сел. Bohuslav",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Bohuslav"


def test_clean_municipality_removes_smt_prefix(spark):
    df = spark.createDataFrame([("смт Irpin",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Irpin"


def test_clean_municipality_expands_rayon_abbreviation(spark):
    df = spark.createDataFrame([("Boryspil р-н",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Boryspil район"
