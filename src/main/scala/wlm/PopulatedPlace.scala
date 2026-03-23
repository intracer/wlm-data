package wlm

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

case class PopulatedPlace(ADM4_EN: String, ADM4_UK: String)

case class AdmName(code: String, name: String)

/**
 * ADM0 through ADM4 represent hierarchical subnational administrative boundaries, ranging from national levels down to local communities.
 * ADM0: National border (e.g., Country).
 * ADM1: First subnational level (e.g., States, Provinces, Regions).
 * ADM2: Second subnational level (e.g., Counties, Districts).
 * ADM3: Third subnational level (e.g., Districts, Sub-districts).
 * ADM4: Fourth subnational level (e.g., Communities, Settlements)
 */
object AdmLevel extends Enumeration {
  val ADM0, ADM1, ADM2, ADM3, ADM4 = Value
}

object Lang extends Enumeration {
  val EN, UK = Value
}

class PopulatedPlaceRepo(spark: SparkSession, lang: Lang.Value) {
  import spark.implicits._

  def populatedPlacesDf(): DataFrame = spark.read
    .option("header", "true")
    .csv("data/humdata/ukraine-populated-places.csv")

  def admNames(admLevel: AdmLevel.Value): Dataset[AdmName] = {
    admNames(populatedPlacesDf(), admLevel).as[AdmName]
  }

  def admNames(df: DataFrame, admLevel: AdmLevel.Value): Dataset[Row] = {
    df
      .select(
        col(s"${admLevel}_PCODE").as("code"),
        col(s"${admLevel}_$lang").as("name")
      )
      .distinct()
      .orderBy(col("code"))
  }

  def dataset(): Dataset[PopulatedPlace] = {
    populatedPlacesDf().as[PopulatedPlace]
  }
}