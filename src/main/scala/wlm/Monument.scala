package wlm

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, _}

case class Monument(id: String,
                    name: String,
                    municipality: Option[String],
                    image: Option[String],
                    adm1: String,
                    adm2: String) {

  def cleanMunicipality: Option[String] = municipality.map(Monument.cleanMunicipality)

  def withCleanMunicipality: Monument = copy(municipality = cleanMunicipality)
}

trait HasAdm {
  def adm: AdmName
}
case class CountPerAdm(adm: AdmName, count: Long) extends HasAdm
case class PercentagePerAdm(adm: AdmName, all: Long, part: Long, percentage: Double) extends HasAdm

object Monument {

  def dataframe(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .csv("data/wiki/monuments/wlm-ua-monuments.csv")
      .drop("adm2")
  }

  def withKoatuuFromId(spark: SparkSession): DataFrame = {
    val adm1Column = concat(
      lit("UA"),
      substring(col("id"), 1, 2)
    )

    val adm2koatuuColumn = concat(
      substring(col("id"), 1, 2),
      substring(col("id"), 4, 3),
      lit("00000")
    )

    dataframe(spark)
    //.filter(col("municipality").isNotNull)
      .withColumns(
        Map(
          "adm1" -> adm1Column,
          "adm2koatuu" -> adm2koatuuColumn
        ))
  }

  def monumentsWithUnmappedKoatuu(spark: SparkSession): Dataset[Monument] = {
    import spark.implicits._
    val katotthKoatuuDf = KatotthKoatuu
      .dataframe(spark)
      .drop("category", "name")

    withKoatuuFromId(spark)
      .join(katotthKoatuuDf, col("adm2koatuu") === col("koatuu"), "left_outer")
      .filter(col("koatuu").isNull)
      .withColumnRenamed("adm2koatuu", "adm2")
      .as[Monument]
  }

  def cleanDataset(spark: SparkSession): Dataset[Monument] = {
    import spark.implicits._

    val katotthKoatuuDf = KatotthKoatuu
      .dataframe(spark)
      .drop("category", "name")

    withKoatuuFromId(spark)
      .withColumnRenamed("adm2koatuu", "adm2")
//      .join(katotthKoatuuDf, col("adm2koatuu") === col("koatuu"))
//      .withColumn("adm2", substring(col("katotth"), 1, 6))
//      .drop("katotth")
      .as[Monument]
      .map(_.withCleanMunicipality)
  }

  def groupByAdm(ds: Dataset[Monument])(implicit spark: SparkSession, lang: Lang.Value): Dataset[CountPerAdm] = {
    import spark.implicits._

    ds.groupBy(col("adm1"))
      .count()
      .join(
        PopulatedPlace.admNames(AdmLevel.ADM1),
        col("adm1") === col("code")
      )
      .select(
        struct(col("code"), col("name")).as("adm"),
        col("count")
      )
      .orderBy(col("count").desc)
      .as[CountPerAdm]
  }

  def numberOfMonumentsByAdm()(implicit spark: SparkSession, lang: Lang.Value): Dataset[CountPerAdm] = {
    groupByAdm(cleanDataset(spark))
  }

  def numberOfPicturedMonumentsByAdm()(implicit spark: SparkSession, lang: Lang.Value): Dataset[CountPerAdm] = {
    groupByAdm(
      cleanDataset(spark)
        .filter(_.image.nonEmpty)
    )
  }

  def percentageOfPicturedMonumentsByAdm1()(implicit spark: SparkSession,
                                            lang: Lang.Value): Dataset[PercentagePerAdm] = {
    import spark.implicits._

    cleanDataset(spark)
      .select(
        col("adm1"),
        when(col("image").isNotNull, 1).otherwise(0).as("pictured")
      )
      .groupBy(col("adm1"))
      .agg(
        sum("pictured").as("pictured"),
        count("pictured").as("count")
      )
      .join(
        PopulatedPlace.admNames(AdmLevel.ADM1),
        col("adm1") === col("code")
      )
      .withColumn(
        "percentage",
        lit(100.0) * col("pictured") / col("count")
      )
      .select(
        struct(col("code"), col("name")).as("adm"),
        col("count").as("all"),
        col("pictured").as("part"),
        col("percentage")
      )
      .orderBy(
        col("percentage").desc
      )
      .as[PercentagePerAdm]

  }

  def cleanMunicipality(raw: String): String = {
    raw
      .replace("р-н", "район")
      .replace("сільська рада", "")
      .replace("селищна рада", "")
      .replace("[[", "")
      .replace("]]", "")
      .replace("&nbsp;", " ")
      .replace('\u00A0', ' ')
      .replace("м.", "")
      .replace("місто", "")
      .replace("с.", "")
      .replace("С.", "")
      .replace(".", "")
      .replace("село", "")
      .replace("сел.", "")
      .replace("смт", "")
      .replace("Смт", "")
      .replace("с-ще", "")
      .replace("с-щ", "")
      .replace("'''", "")
      .replace("''", "")
      .replace(",", "")
      .replace("’", "'")
      .replace("”", "'")
      .split("\\(")
      .head
      .split("\\|")
      .head
      .trim
  }

}
