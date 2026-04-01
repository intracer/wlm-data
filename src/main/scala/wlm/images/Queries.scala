package wlm.images

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Queries {

  private def withRegionNames(df: DataFrame, admNamesDf: DataFrame): DataFrame =
    df.join(admNamesDf, concat(lit("UA"), col("region")) === col("code"), "left")
      .drop("code", "region")
      .withColumnRenamed("name", "region_name")

  /**
    * Cumulative aggregation: approximate distinct monuments per (author, region).
    */
  def cumulativeAgg(df: DataFrame, admNamesDf: DataFrame): DataFrame =
    withRegionNames(
      df.groupBy("author", "region")
        .agg(approx_count_distinct("monument").as("monuments_pictured")),
      admNamesDf
    ).select("author", "region_name", "monuments_pictured")
      .sort(col("monuments_pictured").desc)

  /**
    * Windowed aggregation: approximate distinct monuments per (window, author, region).
    */
  def windowedAgg(df: DataFrame, admNamesDf: DataFrame, windowDuration: String, watermarkDuration: String): DataFrame =
    withRegionNames(
      df.withWatermark("upload_date_ts", watermarkDuration)
        .groupBy(window(col("upload_date_ts"), windowDuration), col("author"), col("region"))
        .agg(approx_count_distinct("monument").as("monuments_pictured")),
      admNamesDf
    ).select("window", "author", "region_name", "monuments_pictured")

}
