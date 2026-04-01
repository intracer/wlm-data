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
