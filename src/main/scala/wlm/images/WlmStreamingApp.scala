package wlm.images

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import wlm.monuments.{AdmLevel, Lang, PopulatedPlaceRepo}

object WlmStreamingApp {

  def main(args: Array[String]): Unit = {
    val cfg = ConfigFactory.load().getConfig("spark-streaming")
    val inputDir = cfg.getString("input-dir")
    val outputDir = cfg.getString("output-dir")
    val checkpointDir = cfg.getString("checkpoint-dir")
    val windowDur = cfg.getString("window-duration")
    val watermarkDur = cfg.getString("watermark-duration")

    val spark = SparkSession
      .builder()
      .appName("WlmStreamingApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val admNamesDf: DataFrame = new PopulatedPlaceRepo(spark, Lang.EN).admNames(AdmLevel.ADM1).toDF()

    val rawStream = spark.readStream
      .schema(WlmSchema.csvSchema)
      .option("header", "true")
      .csv("data/images")

    val transformed = Transformations.transform(rawStream)

    val runQ1 = false
    if (runQ1) {
      cumulativeQuery(outputDir, checkpointDir, transformed, admNamesDf)
    } else {
      windowedAgg(outputDir, checkpointDir, windowDur, watermarkDur, transformed, admNamesDf)
    }

    spark.streams.awaitAnyTermination()
  }

  private def windowedAgg(outputDir: String,
                          checkpointDir: String,
                          windowDur: String,
                          watermarkDur: String,
                          transformed: DataFrame,
                          admNamesDf: DataFrame): Unit = {
    val q2 = Queries
      .windowedAgg(transformed, admNamesDf, windowDur, watermarkDur)
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", s"$checkpointDir/windowed")
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        val sorted = batchDf.sort(col("monuments_pictured").desc)
        sorted.show(truncate = false)
        sorted.write.mode("append").parquet(s"$outputDir/windowed")
      }
      .start()
  }

  private def cumulativeQuery(outputDir: String,
                              checkpointDir: String,
                              transformed: DataFrame,
                              admNamesDf: DataFrame): Unit = {
    val q1 = Queries
      .cumulativeAgg(transformed, admNamesDf)
      .writeStream
      .outputMode("complete")
      .option("checkpointLocation", s"$checkpointDir/cumulative")
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        batchDf.show(truncate = false)
        batchDf.write.mode("overwrite").parquet(s"$outputDir/cumulative")
      }
      .start()
  }
}
