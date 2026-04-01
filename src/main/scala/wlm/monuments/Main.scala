package wlm.monuments

import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .config("spark.master", "local")
    .getOrCreate()
  val lang: Lang.Value = Lang.EN
  val admLevel = AdmLevel.ADM1

  val monumentRepo = new MonumentRepo(spark, lang)

  val monumentsWithCitiesDs = monumentRepo.joinedWithKatotth()
  monumentsWithCitiesDs.show(20, truncate = false)

  monumentsWithCitiesDs.write.parquet("data/wiki/monuments/wlm-ua-with-cities")

  monumentRepo
    .percentageOfPicturedMonumentsByAdm(admLevel)
    .show(30, truncate = false)

}
