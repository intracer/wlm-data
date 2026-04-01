# Databricks PySpark Port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the Scala/Spark wlm-data codebase to PySpark so both pipelines run on Databricks Free Edition, with a TDD test suite that runs locally and on Databricks.

**Architecture:** Business logic lives in importable Python modules under `src/wlm/`; Databricks notebooks in `notebooks/` are thin wrappers. Tests in `tests/` use pytest + a local SparkSession fixture and can also run on Databricks via a test notebook.

**Tech Stack:** PySpark 3.5, pytest, Python 3.9+, Databricks Free Edition (Community Edition)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `requirements.txt` | Create | pyspark + pytest pinned versions |
| `src/wlm/__init__.py` | Create | empty package marker |
| `src/wlm/common.py` | Create | AdmLevel/Lang enums, clean_municipality_col, PopulatedPlaceRepo, KatotthKoatuuRepo |
| `src/wlm/monuments.py` | Create | MonumentRepo |
| `src/wlm/images.py` | Create | WlmSchema, transform(), cumulative_agg(), windowed_agg() |
| `tests/__init__.py` | Create | empty |
| `tests/conftest.py` | Create | session-scoped SparkSession fixture |
| `tests/test_common.py` | Create | tests for enums, clean_municipality_col, PopulatedPlaceRepo, KatotthKoatuuRepo |
| `tests/test_monuments.py` | Create | tests for MonumentRepo |
| `tests/test_images.py` | Create | tests for WlmSchema, transform, cumulative_agg, windowed_agg |
| `notebooks/monuments.py` | Create | Databricks batch pipeline notebook |
| `notebooks/images_streaming.py` | Create | Databricks streaming pipeline notebook |

---

## Task 1: Project scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `src/wlm/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create requirements.txt**

```
pyspark==3.5.6
pytest==8.3.5
```

- [ ] **Step 2: Create src/wlm/__init__.py and tests/__init__.py**

Both files are empty. Create them:

```bash
mkdir -p src/wlm && touch src/wlm/__init__.py
mkdir -p tests && touch tests/__init__.py
```

- [ ] **Step 3: Create tests/conftest.py**

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local")
               .appName("wlm-tests")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
```

- [ ] **Step 4: Install dependencies and verify pytest discovers tests**

```bash
pip install -r requirements.txt
pytest tests/ --collect-only
```

Expected: `no tests ran` (no test files yet), exit 0.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt src/wlm/__init__.py tests/__init__.py tests/conftest.py
git commit -m "feat: add Python project scaffolding for PySpark port"
```

---

## Task 2: AdmLevel and Lang enums

**Files:**
- Create: `src/wlm/common.py`
- Create: `tests/test_common.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_common.py`:

```python
from wlm.common import AdmLevel, Lang


def test_adm_level_values():
    assert AdmLevel.ADM0.value == "ADM0"
    assert AdmLevel.ADM1.value == "ADM1"
    assert AdmLevel.ADM2.value == "ADM2"
    assert AdmLevel.ADM3.value == "ADM3"
    assert AdmLevel.ADM4.value == "ADM4"


def test_lang_values():
    assert Lang.EN.value == "EN"
    assert Lang.UK.value == "UK"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_common.py::test_adm_level_values tests/test_common.py::test_lang_values -v
```

Expected: `ModuleNotFoundError: No module named 'wlm'`

- [ ] **Step 3: Implement enums in src/wlm/common.py**

```python
from enum import Enum


class AdmLevel(Enum):
    ADM0 = "ADM0"
    ADM1 = "ADM1"
    ADM2 = "ADM2"
    ADM3 = "ADM3"
    ADM4 = "ADM4"


class Lang(Enum):
    EN = "EN"
    UK = "UK"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_common.py::test_adm_level_values tests/test_common.py::test_lang_values -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/common.py tests/test_common.py
git commit -m "feat: add AdmLevel and Lang enums"
```

---

## Task 3: clean_municipality_col

**Files:**
- Modify: `src/wlm/common.py`
- Modify: `tests/test_common.py`

- [ ] **Step 1: Append failing tests to tests/test_common.py**

Add at the bottom of `tests/test_common.py`:

```python
from pyspark.sql.functions import col
from wlm.common import clean_municipality_col


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


def test_clean_municipality_removes_smt_prefix(spark):
    df = spark.createDataFrame([("смт Irpin",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Irpin"


def test_clean_municipality_expands_rayon_abbreviation(spark):
    df = spark.createDataFrame([("Boryspil р-н",)], ["raw"])
    result = df.withColumn("clean", clean_municipality_col(col("raw"))).collect()
    assert result[0].clean == "Boryspil район"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_common.py -k "clean_municipality" -v
```

Expected: `ImportError: cannot import name 'clean_municipality_col'`

- [ ] **Step 3: Implement clean_municipality_col in src/wlm/common.py**

Add after the enum definitions:

```python
import pyspark.sql.functions as F
from pyspark.sql import Column


def clean_municipality_col(c: Column) -> Column:
    """
    Pure PySpark column expression equivalent of Monument.cleanMunicipality in Scala.
    Applies a series of regexp_replace operations then extracts the part before
    the first '(' or '|' and trims whitespace.
    """
    # Each tuple is (regex_pattern, replacement).
    # Literal strings with regex special characters are escaped.
    replacements = [
        ("р-н", "район"),
        ("сільська рада", ""),
        ("селищна рада", ""),
        ("\\[\\[", ""),
        ("\\]\\]", ""),
        ("&nbsp;", " "),
        ("\u00A0", " "),    # non-breaking space
        ("м\\.", ""),
        ("місто", ""),
        ("с\\.", ""),
        ("С\\.", ""),
        ("\\.", ""),         # remove all remaining dots
        ("село", ""),
        ("смт", ""),
        ("Смт", ""),
        ("с-ще", ""),
        ("с-щ", ""),
        ("'''", ""),
        ("''", ""),
        (",", ""),
        ("\u2019", "'"),    # right single quotation mark → apostrophe
        ("\u201c", "'"),    # left double quotation mark → apostrophe
    ]
    result = c
    for pattern, replacement in replacements:
        result = F.regexp_replace(result, pattern, replacement)
    # Mirror Scala's .split("\\(").head.split("\\|").head.trim:
    # capture everything before the first '(' or '|', then trim.
    result = F.trim(F.regexp_extract(result, r"^([^(|]*)", 1))
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_common.py -k "clean_municipality" -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/common.py tests/test_common.py
git commit -m "feat: add clean_municipality_col column expression"
```

---

## Task 4: PopulatedPlaceRepo

**Files:**
- Modify: `src/wlm/common.py`
- Modify: `tests/test_common.py`

These are integration tests that read `data/humdata/ukraine-populated-places.csv` — the same file the Scala tests use.

- [ ] **Step 1: Append failing tests to tests/test_common.py**

```python
from wlm.common import PopulatedPlaceRepo


def test_populated_place_adm0_is_ukraine(spark):
    repo = PopulatedPlaceRepo(spark, Lang.EN)
    result = repo.adm_names(AdmLevel.ADM0).collect()
    assert len(result) == 1
    assert result[0].code == "UA"
    assert result[0].name == "Ukraine"


def test_populated_place_adm1_has_27_regions(spark):
    repo = PopulatedPlaceRepo(spark, Lang.EN)
    result = repo.adm_names(AdmLevel.ADM1).collect()
    assert len(result) == 27


def test_populated_place_adm4_contains_major_cities(spark):
    repo = PopulatedPlaceRepo(spark, Lang.EN)
    names = {r.name for r in repo.adm_names(AdmLevel.ADM4).collect()}
    assert len(names) > 29000
    assert {"Kyiv", "Kharkiv", "Lviv", "Odesa", "Dnipro"}.issubset(names)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_common.py -k "populated_place" -v
```

Expected: `ImportError: cannot import name 'PopulatedPlaceRepo'`

- [ ] **Step 3: Implement PopulatedPlaceRepo in src/wlm/common.py**

Add after `clean_municipality_col`:

```python
from pyspark.sql import DataFrame, SparkSession


class PopulatedPlaceRepo:
    DEFAULT_PATH = "data/humdata/ukraine-populated-places.csv"

    def __init__(self, spark: SparkSession, lang: "Lang", path: str = DEFAULT_PATH):
        self._spark = spark
        self._lang = lang
        self._path = path

    def populated_places_df(self) -> DataFrame:
        return self._spark.read.option("header", "true").csv(self._path)

    def adm_names(self, adm_level: "AdmLevel") -> DataFrame:
        level = adm_level.value
        lang = self._lang.value
        return (self.populated_places_df()
                .select(
                    F.col(f"{level}_PCODE").alias("code"),
                    F.col(f"{level}_{lang}").alias("name")
                )
                .distinct()
                .orderBy("code"))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_common.py -k "populated_place" -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/common.py tests/test_common.py
git commit -m "feat: add PopulatedPlaceRepo"
```

---

## Task 5: KatotthKoatuuRepo

**Files:**
- Modify: `src/wlm/common.py`
- Modify: `tests/test_common.py`

Integration tests read `data/katotth/katotth_koatuu.csv`.

- [ ] **Step 1: Append failing tests to tests/test_common.py**

```python
from wlm.common import KatotthKoatuuRepo


def test_unique_name_by_adm2_has_no_duplicate_prefix_name_pairs(spark):
    repo = KatotthKoatuuRepo(spark)
    result = repo.unique_name_by_adm2()
    # Every (koatuuPrefix, name) pair must appear exactly once
    max_cnt = (result
               .groupBy("koatuuPrefix", "name")
               .agg(F.count("*").alias("cnt"))
               .agg(F.max("cnt"))
               .collect()[0][0])
    assert max_cnt == 1


def test_non_unique_name_by_adm2_is_non_empty(spark):
    repo = KatotthKoatuuRepo(spark)
    assert repo.non_unique_name_by_adm2().count() > 0
```

Note: `F` is already imported in `test_common.py` via `from wlm.common import F` — no, actually `F` is not exported from `common.py`. Add this import at the top of `test_common.py`:

```python
import pyspark.sql.functions as F
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_common.py -k "katotth" -v
```

Expected: `ImportError: cannot import name 'KatotthKoatuuRepo'`

- [ ] **Step 3: Implement KatotthKoatuuRepo in src/wlm/common.py**

Add after `PopulatedPlaceRepo`:

```python
class KatotthKoatuuRepo:
    DEFAULT_PATH = "data/katotth/katotth_koatuu.csv"

    def __init__(self, spark: SparkSession, path: str = DEFAULT_PATH):
        self._spark = spark
        self._path = path

    def dataframe(self) -> DataFrame:
        return self._spark.read.option("header", "true").csv(self._path)

    def grouped_by_adm2_and_name(self) -> DataFrame:
        return (self.dataframe()
                .withColumn("koatuuPrefix", F.substring(F.col("koatuu"), 1, 5))
                .groupBy("koatuuPrefix", "name")
                .agg(
                    F.count("*").alias("cnt"),
                    F.first("katotth").alias("katotth"),
                    F.first("koatuu").alias("koatuu"),
                    F.first("category").alias("category")
                ))

    def unique_name_by_adm2(self) -> DataFrame:
        return (self.grouped_by_adm2_and_name()
                .filter(F.col("cnt") == 1)
                .drop("cnt"))

    def non_unique_name_by_adm2(self) -> DataFrame:
        return (self.grouped_by_adm2_and_name()
                .filter(F.col("cnt") > 1))
```

Also add `import pyspark.sql.functions as F` at the top of `src/wlm/common.py` (before the class definitions, after the `from enum import Enum` line).

- [ ] **Step 4: Run all test_common.py tests to verify everything passes**

```bash
pytest tests/test_common.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/common.py tests/test_common.py
git commit -m "feat: add KatotthKoatuuRepo"
```

---

## Task 6: MonumentRepo — data loading and KOATUU derivation

**Files:**
- Create: `src/wlm/monuments.py`
- Create: `tests/test_monuments.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_monuments.py`:

```python
import pytest
from pyspark.sql import SparkSession
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
    df = spark.createDataFrame(
        [("14-101-0001", "Monument", "Kyiv", None)],
        ["id", "name", "municipality", "image"]
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA14"
    assert result[0].adm2koatuu == "1410100000"


def test_with_koatuu_from_id_kyiv_special_case(spark):
    """Kyiv (80): middle segment is always '000' regardless of id."""
    df = spark.createDataFrame(
        [("80-001-0001", "Monument", "Kyiv", None)],
        ["id", "name", "municipality", "image"]
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA80"
    assert result[0].adm2koatuu == "8000000000"


def test_with_koatuu_from_id_sevastopol_special_case(spark):
    """Sevastopol (85): middle segment is always '000' regardless of id."""
    df = spark.createDataFrame(
        [("85-001-0001", "Monument", "Sevastopol", None)],
        ["id", "name", "municipality", "image"]
    )
    result = MonumentRepo(spark, Lang.EN)._with_koatuu_from_id_df(df).collect()
    assert result[0].adm1 == "UA85"
    assert result[0].adm2koatuu == "8500000000"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_monuments.py -v
```

Expected: `ModuleNotFoundError: No module named 'wlm.monuments'`

- [ ] **Step 3: Create src/wlm/monuments.py with dataframe() and _with_koatuu_from_id_df()**

```python
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from wlm.common import AdmLevel, KatotthKoatuuRepo, Lang, PopulatedPlaceRepo, clean_municipality_col


class MonumentRepo:
    DEFAULT_PATH = "data/wiki/monuments/wlm-ua-monuments.csv"

    def __init__(
        self,
        spark: SparkSession,
        lang: Lang,
        path: str = DEFAULT_PATH,
        humdata_path: str = PopulatedPlaceRepo.DEFAULT_PATH,
        katotth_path: str = KatotthKoatuuRepo.DEFAULT_PATH,
    ):
        self._spark = spark
        self._lang = lang
        self._path = path
        self._populated_place_repo = PopulatedPlaceRepo(spark, lang, path=humdata_path)
        self._katotth_koatuu_repo = KatotthKoatuuRepo(spark, path=katotth_path)

    def dataframe(self) -> DataFrame:
        return (self._spark.read
                .option("header", "true")
                .csv(self._path)
                .drop("adm2"))

    def with_koatuu_from_id(self) -> DataFrame:
        return self._with_koatuu_from_id_df(self.dataframe())

    def _with_koatuu_from_id_df(self, df: DataFrame) -> DataFrame:
        """Applies KOATUU derivation to an arbitrary DataFrame with an 'id' column.
        Extracted as a separate method so tests can inject small DataFrames."""
        adm1_col = F.concat(F.lit("UA"), F.substring(F.col("id"), 1, 2))
        adm2_koatuu_col = F.concat(
            F.substring(F.col("id"), 1, 2),
            F.when(
                F.substring(F.col("id"), 1, 2).isin("80", "85"),
                "000"
            ).otherwise(
                F.substring(F.col("id"), 4, 3)
            ),
            F.lit("00000")
        )
        return (df
                .withColumn("adm1", adm1_col)
                .withColumn("adm2koatuu", adm2_koatuu_col)
                .withColumn("adm3", F.lit(None).cast("string"))
                .withColumn("adm4", F.lit(None).cast("string")))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_monuments.py::test_with_koatuu_from_id_standard_region \
       tests/test_monuments.py::test_with_koatuu_from_id_kyiv_special_case \
       tests/test_monuments.py::test_with_koatuu_from_id_sevastopol_special_case -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/monuments.py tests/test_monuments.py
git commit -m "feat: add MonumentRepo data loading and KOATUU derivation"
```

---

## Task 7: MonumentRepo — cleaned_municipality_dataset and monuments_with_unmapped_koatuu

**Files:**
- Modify: `src/wlm/monuments.py`
- Modify: `tests/test_monuments.py`

- [ ] **Step 1: Append failing tests to tests/test_monuments.py**

```python
def test_cleaned_municipality_dataset_non_empty(spark):
    repo = MonumentRepo(spark, Lang.EN)
    result = repo.cleaned_municipality_dataset()
    assert result.count() > 0
    # municipality column exists and is not all null
    non_null = result.filter(F.col("municipality").isNotNull()).count()
    assert non_null > 0


def test_monuments_with_unmapped_koatuu_has_groups(spark):
    repo = MonumentRepo(spark, Lang.EN)
    result = repo.monuments_with_unmapped_koatuu()
    groups = result.groupBy("adm1", "adm2").count().collect()
    assert len(groups) > 0
```

Add at the top of `test_monuments.py`:

```python
import pyspark.sql.functions as F
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_monuments.py -k "cleaned_municipality or unmapped" -v
```

Expected: `AttributeError: 'MonumentRepo' object has no attribute 'cleaned_municipality_dataset'`

- [ ] **Step 3: Add methods to MonumentRepo in src/wlm/monuments.py**

Add these two methods inside `MonumentRepo` after `_with_koatuu_from_id_df`:

```python
    def cleaned_municipality_dataset(self) -> DataFrame:
        return (self.with_koatuu_from_id()
                .withColumnRenamed("adm2koatuu", "adm2")
                .withColumn("municipality", clean_municipality_col(F.col("municipality"))))

    def monuments_with_unmapped_koatuu(self) -> DataFrame:
        katotth_df = self._katotth_koatuu_repo.dataframe().drop("category", "name")
        return (self.with_koatuu_from_id()
                .join(
                    katotth_df,
                    F.col("adm2koatuu") == F.col("koatuu"),
                    "left_outer"
                )
                .filter(F.col("koatuu").isNull())
                .withColumnRenamed("adm2koatuu", "adm2"))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_monuments.py -k "cleaned_municipality or unmapped" -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/monuments.py tests/test_monuments.py
git commit -m "feat: add cleaned_municipality_dataset and monuments_with_unmapped_koatuu"
```

---

## Task 8: MonumentRepo — joined_with_katotth

**Files:**
- Modify: `src/wlm/monuments.py`
- Modify: `tests/test_monuments.py`

- [ ] **Step 1: Append failing test to tests/test_monuments.py**

```python
def test_joined_with_katotth_has_adm_columns(spark):
    repo = MonumentRepo(spark, Lang.EN)
    result = repo.joined_with_katotth()
    assert result.count() > 0
    col_names = set(result.columns)
    assert {"id", "name", "image", "adm1", "adm2", "adm3", "adm4", "municipality"}.issubset(col_names)
    # adm1 values should all be valid UA codes
    adm1_values = {r.adm1 for r in result.select("adm1").distinct().collect()}
    assert adm1_values.issubset(ADM1_CODES)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_monuments.py::test_joined_with_katotth_has_adm_columns -v
```

Expected: `AttributeError: 'MonumentRepo' object has no attribute 'joined_with_katotth'`

- [ ] **Step 3: Add joined_with_katotth to MonumentRepo**

Add after `monuments_with_unmapped_koatuu`:

```python
    def joined_with_katotth(self) -> DataFrame:
        monuments = self.cleaned_municipality_dataset()

        unique_by_prefix = (self._katotth_koatuu_repo.unique_name_by_adm2()
                            .withColumnRenamed("name", "municipality_name")
                            .drop("koatuu", "category"))

        adm4_names = (self._populated_place_repo.adm_names(AdmLevel.ADM4)
                      .withColumnRenamed("name", "municipality_name"))

        return (monuments
                .join(
                    unique_by_prefix,
                    (F.substring(F.col("adm2"), 1, 5) == F.col("koatuuPrefix")) &
                    (F.col("municipality") == F.col("municipality_name"))
                )
                .drop("koatuuPrefix", "municipality", "adm2", "adm4", "municipality_name")
                .join(
                    adm4_names,
                    F.substring(F.col("katotth"), 1, 12) == F.col("code")
                )
                .withColumn("adm2", F.substring(F.col("code"), 1, 6))
                .withColumn("adm3", F.substring(F.col("code"), 1, 9))
                .withColumnsRenamed({"municipality_name": "municipality", "code": "adm4"}))
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_monuments.py::test_joined_with_katotth_has_adm_columns -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/monuments.py tests/test_monuments.py
git commit -m "feat: add MonumentRepo.joined_with_katotth"
```

---

## Task 9: MonumentRepo — statistics methods

**Files:**
- Modify: `src/wlm/monuments.py`
- Modify: `tests/test_monuments.py`

- [ ] **Step 1: Append failing tests to tests/test_monuments.py**

```python
def test_number_of_monuments_by_adm1(spark):
    repo = MonumentRepo(spark, Lang.EN)
    rows = repo.number_of_monuments_by_adm(AdmLevel.ADM1).collect()
    codes = {r.adm["code"] for r in rows}
    assert codes == ADM1_CODES
    counts = [r["count"] for r in rows]
    assert counts == sorted(counts, reverse=True), "expected descending order"
    assert all(c > 500 for c in counts)
    assert all(c < 15000 for c in counts)
    assert sum(counts) > 95000


def test_number_of_pictured_monuments_by_adm1(spark):
    repo = MonumentRepo(spark, Lang.EN)
    rows = repo.number_of_pictured_monuments_by_adm(AdmLevel.ADM1).collect()
    codes = {r.adm["code"] for r in rows}
    assert codes == ADM1_CODES
    counts = [r["count"] for r in rows]
    assert counts == sorted(counts, reverse=True), "expected descending order"
    assert all(c > 400 for c in counts)
    assert all(c < 5000 for c in counts)
    assert sum(counts) > 35000


def test_percentage_of_pictured_monuments_by_adm1(spark):
    repo = MonumentRepo(spark, Lang.EN)
    rows = repo.percentage_of_pictured_monuments_by_adm(AdmLevel.ADM1).collect()
    codes = {r.adm["code"] for r in rows}
    assert codes == ADM1_CODES
    percentages = [r.percentage for r in rows]
    assert percentages == sorted(percentages, reverse=True), "expected descending order"
    # spot-check: percentage == 100 * part / all within 0.01 tolerance
    for row in rows:
        expected = 100.0 * row.part / row.all
        assert abs(row.percentage - expected) < 0.01, f"wrong percentage for {row.adm}"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_monuments.py -k "number_of_monuments or number_of_pictured or percentage" -v
```

Expected: `AttributeError` for missing methods.

- [ ] **Step 3: Add statistics methods to MonumentRepo**

Add after `joined_with_katotth`:

```python
    def group_by_adm(self, df: DataFrame, adm_level: AdmLevel) -> DataFrame:
        adm_col = F.col(adm_level.value.lower())
        return (df.groupBy(adm_col)
                .count()
                .join(
                    self._populated_place_repo.adm_names(adm_level),
                    adm_col == F.col("code")
                )
                .select(
                    F.struct(F.col("code"), F.col("name")).alias("adm"),
                    F.col("count")
                )
                .orderBy(F.col("count").desc()))

    def number_of_monuments_by_adm(self, adm_level: AdmLevel) -> DataFrame:
        return self.group_by_adm(self.joined_with_katotth(), adm_level)

    def number_of_pictured_monuments_by_adm(self, adm_level: AdmLevel) -> DataFrame:
        return self.group_by_adm(
            self.joined_with_katotth().filter(F.col("image").isNotNull()),
            adm_level
        )

    def percentage_of_pictured_monuments_by_adm(self, adm_level: AdmLevel) -> DataFrame:
        adm_col = F.col(adm_level.value.lower())
        return (self.joined_with_katotth()
                .select(
                    adm_col,
                    F.when(F.col("image").isNotNull(), 1).otherwise(0).alias("pictured")
                )
                .groupBy(adm_col)
                .agg(
                    F.sum("pictured").alias("pictured"),
                    F.count("pictured").alias("count")
                )
                .join(
                    self._populated_place_repo.adm_names(adm_level),
                    adm_col == F.col("code")
                )
                .withColumn("percentage", F.lit(100.0) * F.col("pictured") / F.col("count"))
                .select(
                    F.struct(F.col("code"), F.col("name")).alias("adm"),
                    F.col("count").alias("all"),
                    F.col("pictured").alias("part"),
                    F.col("percentage")
                )
                .orderBy(F.col("percentage").desc()))
```

- [ ] **Step 4: Run all monument tests to verify they pass**

```bash
pytest tests/test_monuments.py -v
```

Expected: all tests pass. Note: integration tests reading real CSVs may take 30–60 seconds.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/monuments.py tests/test_monuments.py
git commit -m "feat: add MonumentRepo statistics methods"
```

---

## Task 10: WlmSchema and transform

**Files:**
- Create: `src/wlm/images.py`
- Create: `tests/test_images.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_images.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_images.py -v
```

Expected: `ModuleNotFoundError: No module named 'wlm.images'`

- [ ] **Step 3: Create src/wlm/images.py with WlmSchema and transform()**

```python
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
    """
    return (df
            .withColumn("upload_date_ts", F.to_timestamp(F.col("upload_date")))
            .withColumn("monument", F.explode(F.split(F.col("monument_id"), ";")))
            .filter(F.col("monument") != "")
            .withColumn("region", F.regexp_extract(F.col("monument"), r"^(\d+)", 1))
            .select("author", "monument", "region", "upload_date_ts"))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_images.py -k "transform" -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/images.py tests/test_images.py
git commit -m "feat: add WlmSchema and transform()"
```

---

## Task 11: cumulative_agg and windowed_agg

**Files:**
- Modify: `src/wlm/images.py`
- Modify: `tests/test_images.py`

- [ ] **Step 1: Append failing tests to tests/test_images.py**

```python
import datetime
from pyspark.sql.types import StructType, StructField, StringType
from wlm.images import cumulative_agg, windowed_agg


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

    # Write a small test CSV matching transformed_schema
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_images.py -k "cumulative or windowed" -v
```

Expected: `ImportError: cannot import name 'cumulative_agg'`

- [ ] **Step 3: Add cumulative_agg and windowed_agg to src/wlm/images.py**

```python
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
```

- [ ] **Step 4: Run all image tests to verify they pass**

```bash
pytest tests/test_images.py -v
```

Expected: all tests pass. The smoke test may take up to 30 seconds for streaming setup.

- [ ] **Step 5: Commit**

```bash
git add src/wlm/images.py tests/test_images.py
git commit -m "feat: add cumulative_agg and windowed_agg"
```

---

## Task 12: Monuments Databricks notebook

**Files:**
- Create: `notebooks/monuments.py`

No TDD for notebooks — verified manually on Databricks.

- [ ] **Step 1: Create notebooks directory and monuments notebook**

```bash
mkdir -p notebooks
```

Create `notebooks/monuments.py`:

```python
# Databricks notebook source
# This notebook runs the monuments batch pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - CSV files uploaded to DBFS at the paths below

import sys
sys.path.insert(0, "src")

# COMMAND ----------

from wlm.common import AdmLevel, Lang
from wlm.monuments import MonumentRepo

# COMMAND ----------
# Config — edit DBFS paths as needed

MONUMENTS_CSV  = "dbfs:/FileStore/wlm-data/monuments/wlm-ua-monuments.csv"
HUMDATA_CSV    = "dbfs:/FileStore/wlm-data/humdata/ukraine-populated-places.csv"
KATOTTH_CSV    = "dbfs:/FileStore/wlm-data/katotth/katotth_koatuu.csv"
OUTPUT_DIR     = "dbfs:/FileStore/wlm-data/output/monuments-with-cities"

# COMMAND ----------
# spark is pre-created by Databricks — no SparkSession.builder needed

repo = MonumentRepo(
    spark,
    Lang.EN,
    path=MONUMENTS_CSV,
    humdata_path=HUMDATA_CSV,
    katotth_path=KATOTTH_CSV,
)

# COMMAND ----------
# Write monuments joined with geographic data to parquet

joined = repo.joined_with_katotth()
joined.write.mode("overwrite").parquet(OUTPUT_DIR)
print(f"Written to {OUTPUT_DIR}")

# COMMAND ----------
# Show pictured monument percentage by region (ADM1)

repo.percentage_of_pictured_monuments_by_adm(AdmLevel.ADM1).show(30, truncate=False)
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/monuments.py
git commit -m "feat: add Databricks monuments batch notebook"
```

---

## Task 13: Images streaming Databricks notebook

**Files:**
- Create: `notebooks/images_streaming.py`

- [ ] **Step 1: Create notebooks/images_streaming.py**

```python
# Databricks notebook source
# This notebook runs the images structured streaming pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Image CSV files dropped into dbfs:/FileStore/wlm-data/images/
#   - Run ONCE per session; re-run to pick up newly added CSV files
#     (trigger=availableNow processes all available files then terminates)

import sys
sys.path.insert(0, "src")

# COMMAND ----------

from wlm.common import AdmLevel, Lang, PopulatedPlaceRepo
from wlm.images import WlmSchema, transform, windowed_agg, cumulative_agg

# COMMAND ----------
# Config — edit as needed

INPUT_DIR      = "dbfs:/FileStore/wlm-data/images"
HUMDATA_CSV    = "dbfs:/FileStore/wlm-data/humdata/ukraine-populated-places.csv"
OUTPUT_DIR     = "dbfs:/FileStore/wlm-data/output"
CHECKPOINT_DIR = "dbfs:/FileStore/wlm-data/checkpoints"
WINDOW_DUR     = "1 hour"
WATERMARK_DUR  = "10 minutes"

# Toggle: set to True to run the cumulative query instead of windowed
RUN_CUMULATIVE = False

# COMMAND ----------

adm_names_df = PopulatedPlaceRepo(spark, Lang.EN, path=HUMDATA_CSV).adm_names(AdmLevel.ADM1)

raw_stream = (spark.readStream
              .schema(WlmSchema.csv_schema)
              .option("header", "true")
              .csv(INPUT_DIR))

transformed = transform(raw_stream)

# COMMAND ----------

if RUN_CUMULATIVE:
    query = (cumulative_agg(transformed, adm_names_df)
             .writeStream
             .outputMode("complete")
             .option("checkpointLocation", f"{CHECKPOINT_DIR}/cumulative")
             .trigger(availableNow=True)
             .foreachBatch(lambda df, _: (
                 df.show(truncate=False),
                 df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/cumulative")
             ))
             .start())
else:
    query = (windowed_agg(transformed, adm_names_df, WINDOW_DUR, WATERMARK_DUR)
             .writeStream
             .outputMode("append")
             .option("checkpointLocation", f"{CHECKPOINT_DIR}/windowed")
             .trigger(availableNow=True)
             .foreachBatch(lambda df, _: (
                 df.sort("monuments_pictured", ascending=False).show(truncate=False),
                 df.write.mode("append").parquet(f"{OUTPUT_DIR}/windowed")
             ))
             .start())

# COMMAND ----------
# Wait for the triggered run to finish (availableNow=True terminates after processing all files)

query.awaitTermination()
print("Streaming query complete.")
```

- [ ] **Step 2: Run the full test suite one final time**

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add notebooks/images_streaming.py
git commit -m "feat: add Databricks images streaming notebook"
```

---

## Task 14: Databricks test notebook

**Files:**
- Create: `notebooks/run_tests.py`

This notebook runs the pytest suite on Databricks, satisfying the "tests run on Databricks" requirement from the spec.

- [ ] **Step 1: Create notebooks/run_tests.py**

```python
# Databricks notebook source
# Runs the full pytest test suite on the Databricks cluster.
# Prerequisites: repo cloned via Databricks Repos, local CSV files present in data/

# COMMAND ----------
%pip install pytest

# COMMAND ----------
import sys
sys.path.insert(0, "src")

# COMMAND ----------
import pytest

# Run tests and print results. Exit code 0 = all passed.
retcode = pytest.main(["tests/", "-v", "--tb=short"])
assert retcode == 0, f"pytest exited with code {retcode} — see output above"
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/run_tests.py
git commit -m "feat: add Databricks test runner notebook"
```

---

## Databricks Setup Checklist

After the implementation is complete, follow these steps to run on Databricks Free Edition:

1. **Connect repo:** In Databricks workspace → Repos → Add Repo → paste the GitHub URL of this repo
2. **Upload CSVs** via Data > Add Data > DBFS:
   - `data/wiki/monuments/wlm-ua-monuments.csv` → `/FileStore/wlm-data/monuments/wlm-ua-monuments.csv`
   - `data/humdata/ukraine-populated-places.csv` → `/FileStore/wlm-data/humdata/ukraine-populated-places.csv`
   - `data/katotth/katotth_koatuu.csv` → `/FileStore/wlm-data/katotth/katotth_koatuu.csv`
3. **Start a cluster** (single-node is fine for Free Edition)
4. **Open `notebooks/monuments.py`** and click Run All
5. **For streaming:** drop image CSVs into `/FileStore/wlm-data/images/`, then open and run `notebooks/images_streaming.py`
