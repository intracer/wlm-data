package wlm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .config("spark.master", "local")
    .getOrCreate()
  val lang: Lang.Value = Lang.EN
  val admLevel = AdmLevel.ADM1

  val outputPath = if (args.nonEmpty) args(0) else "data/processed/spark"

  val monumentRepo = new MonumentRepo(spark, lang)

  val monumentsWithCitiesDs = monumentRepo.joinedWithKatotth()
  monumentsWithCitiesDs.show(20, truncate = false)

  monumentsWithCitiesDs.write.mode("overwrite").parquet(outputPath)

  monumentRepo
    .percentageOfPicturedMonumentsByAdm(admLevel)
    .show(30, truncate = false)
}
