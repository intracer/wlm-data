package wlm

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, _}

case class Monument(id: String,
                    name: String,
                    municipality: Option[String],
                    image: Option[String],
                    adm1: String,
                    adm2: String,
                    adm4: Option[String]) {

  def cleanMunicipality: Option[String] = municipality.map(Monument.cleanMunicipality)

  def withCleanMunicipality: Monument = copy(municipality = cleanMunicipality)
}

trait HasAdm {
  def adm: AdmName
}
case class CountPerAdm(adm: AdmName, count: Long) extends HasAdm
case class PercentagePerAdm(adm: AdmName, all: Long, part: Long, percentage: Double) extends HasAdm

class MonumentRepo(spark: SparkSession, lang: Lang.Value) {
  import spark.implicits._

  lazy val populatedPlaceRepo = new PopulatedPlaceRepo(spark, lang)
  lazy val katotthKoatuuRepo = new KatotthKoatuuRepo(spark)

  def dataframe(): DataFrame = {
    spark.read
      .option("header", "true")
      .csv("data/wiki/monuments/wlm-ua-monuments.csv")
      .drop("adm2")
  }

  def withKoatuuFromId(): DataFrame = {
    val adm1Column = concat(
      lit("UA"),
      substring(col("id"), 1, 2)
    )

    val adm2koatuuColumn = concat(
      substring(col("id"), 1, 2),
      when(
        substring(col("id"), 1, 2) isInCollection Seq("80", "85"), // Special cases for Kyiv and Sevastopol
        "000"
      ).otherwise(
          substring(col("id"), 4, 3)
        ),
      lit("00000")
    )

    dataframe()
    //.filter(col("municipality").isNotNull)
      .withColumns(
        Map(
          "adm1" -> adm1Column,
          "adm2koatuu" -> adm2koatuuColumn,
          "adm4" -> lit(null)
        ))
  }

  def monumentsWithUnmappedKoatuu(): Dataset[Monument] = {
    val katotthKoatuuDf = katotthKoatuuRepo
      .dataframe()
      .drop("category", "name")

    withKoatuuFromId()
      .join(katotthKoatuuDf, col("adm2koatuu") === col("koatuu"), "left_outer")
      .filter(col("koatuu").isNull)
      .withColumnRenamed("adm2koatuu", "adm2")
      .as[Monument]
  }

  def joinedWithKatotth(): Dataset[Monument] = {
    val monuments = cleanDataset()
    val uniqueByPrefix = katotthKoatuuRepo
      .uniqueNameByAdm2()
      .withColumnRenamed("name", "municipality_name")
      .drop("koatuu", "category")

    monuments
      .join(
        uniqueByPrefix,
        substring(col("adm2"), 1, 5) === col("koatuuPrefix") && col("municipality") === col("municipality_name")
      )
      .drop("koatuuPrefix", "municipality", "adm2", "adm4", "municipality_name")
      .join(
        populatedPlaceRepo
          .admNames(AdmLevel.ADM4)
          .withColumnRenamed("name", "municipality_name"),
        substring(col("katotth"), 1, 12) === col("code")
      )
      .withColumn("adm2", substring(col("code"), 1, 6))
      .withColumnsRenamed(
        Map(
          "municipality_name" -> "municipality",
          "code" -> "adm4",
        ))
      .as[Monument]
  }

  def cleanDataset(): Dataset[Monument] = {
    import spark.implicits._

    withKoatuuFromId()
      .withColumnRenamed("adm2koatuu", "adm2")
      .as[Monument]
      .map(_.withCleanMunicipality)
  }

  def groupByAdm(ds: Dataset[Monument], admLevel: AdmLevel.Value): Dataset[CountPerAdm] = {
    val monumentAdmCol = col(admLevel.toString.toLowerCase)

    ds.groupBy(monumentAdmCol)
      .count()
      .join(
        populatedPlaceRepo.admNames(admLevel),
        monumentAdmCol === col("code")
      )
      .select(
        struct(col("code"), col("name")).as("adm"),
        col("count")
      )
      .orderBy(col("count").desc)
      .as[CountPerAdm]
  }

  def numberOfMonumentsByAdm(admLevel: AdmLevel.Value): Dataset[CountPerAdm] = {
    groupByAdm(joinedWithKatotth().as[Monument], admLevel)
  }

  def numberOfPicturedMonumentsByAdm(admLevel: AdmLevel.Value): Dataset[CountPerAdm] = {
    groupByAdm(
      joinedWithKatotth().as[Monument].filter(_.image.nonEmpty),
      admLevel
    )
  }

  def percentageOfPicturedMonumentsByAdm(admLevel: AdmLevel.Value): Dataset[PercentagePerAdm] = {
    val monumentAdmCol = col(admLevel.toString.toLowerCase)

    cleanDataset()
      .select(
        monumentAdmCol,
        when(col("image").isNotNull, 1).otherwise(0).as("pictured")
      )
      .groupBy(monumentAdmCol)
      .agg(
        sum("pictured").as("pictured"),
        count("pictured").as("count")
      )
      .join(
        populatedPlaceRepo.admNames(AdmLevel.ADM1),
        monumentAdmCol === col("code")
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
}

object Monument {
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
