package wlm.images

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Queries {

  /**
   * Cumulative aggregation: approximate distinct monuments per (author, region).
   */
  def cumulativeAgg(df: DataFrame): DataFrame =
    df.groupBy("author", "region")
      .agg(approx_count_distinct("monument").as("monuments_pictured"))
      .sort(col("monuments_pictured").desc)

  /**
   * Windowed aggregation: approximate distinct monuments per (window, author, region).
   */
  def windowedAgg(df: DataFrame, windowDuration: String, watermarkDuration: String): DataFrame =
    df.withWatermark("upload_date_ts", watermarkDuration)
      .groupBy(window(col("upload_date_ts"), windowDuration), col("author"), col("region"))
      .agg(approx_count_distinct("monument").as("monuments_pictured"))
}
