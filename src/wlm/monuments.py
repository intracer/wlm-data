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

    def joined_with_katotth(self) -> DataFrame:
        monuments = self.cleaned_municipality_dataset()

        unique_by_prefix = (self._katotth_koatuu_repo.unique_name_by_adm2()
                            .withColumnRenamed("name", "municipality_name")
                            .drop("koatuu", "category"))

        adm4_names = (self._populated_place_repo.adm_names(AdmLevel.ADM4)
                      .withColumnRenamed("name", "municipality_name"))

        # Inner joins: monuments that cannot be mapped to a KATOTTH entry or an
        # ADM4 place name are intentionally excluded from the result.
        # Use monuments_with_unmapped_koatuu() to inspect what was excluded.
        return (monuments
                .join(
                    unique_by_prefix,
                    (F.substring(F.col("adm2"), 1, 5) == F.col("koatuuPrefix")) &
                    (F.col("municipality") == F.col("municipality_name"))
                )
                .drop("koatuuPrefix", "municipality", "adm2", "adm3", "adm4", "municipality_name")
                .join(
                    adm4_names,
                    F.substring(F.col("katotth"), 1, 12) == F.col("code")
                )
                .withColumn("adm2", F.substring(F.col("code"), 1, 6))
                .withColumn("adm3", F.substring(F.col("code"), 1, 9))
                .withColumnsRenamed({"municipality_name": "municipality", "code": "adm4"}))

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
