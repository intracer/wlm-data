package wlm

import org.apache.spark.sql.{DataFrame, SparkSession}

case class KatotthKoatuu(katotth: String, koatuu: String, category: String, name: String)

object KatotthKoatuu {

  def dataframe(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .csv("data/katotth/katotth_koatuu.csv")
  }

}
