package wlm

import org.apache.spark.sql.functions.{col, count, first, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class KatotthKoatuu(katotth: String, koatuu: String, category: String, name: String)

class KatotthKoatuuRepo(spark: SparkSession) {

  def dataframe(): DataFrame = {
    spark.read
      .option("header", "true")
      .csv("data/katotth/katotth_koatuu.csv")
  }

  def groupedByAdm2AndName(): DataFrame = {
    dataframe()
      .withColumn("koatuuPrefix", substring(col("koatuu"), 1, 5))
      .groupBy("koatuuPrefix", "name")
      .agg(
        count("*").as("cnt"),
        first("katotth").as("katotth"),
        first("koatuu").as("koatuu"),
        first("category").as("category")
      )
  }

  def uniqueNameByAdm2(): DataFrame = {
    groupedByAdm2AndName()
      .filter(col("cnt") === 1)
      .drop("cnt")
  }

  def nonUniqueNameByAdm2(): DataFrame = {
    groupedByAdm2AndName()
      .filter(col("cnt") > 1)
  }

}
