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

  val monumentRepo = new MonumentRepo(spark, lang)
  val populatedPlace = monumentRepo.populatedPlaceRepo

  monumentRepo
    .percentageOfPicturedMonumentsByAdm1()
    .show(30, truncate = false)

  monumentRepo
    .percentageOfPicturedMonumentsByAdm1()
    .show(30, truncate = false)

  sys.exit()

  val ukrainePopulatedPlaceDs = populatedPlace.dataset()

  val katotthKoatuuDs = spark.read
    .option("header", "true")
    .csv("data/katotth/katotth_koatuu.csv")

  //  monumentsDs.join(ukrainePopulatedPlaceDs.filter(col("ADM1_CAPITAL") === lit("1")), col("adm1") === col("ADM1_PCODE")).show()

  sys.exit()

  // katotthKoatuuDs.show()
  //  ukrainePopulatedPlaceDs.show()

  //  println("Count of Kyiv in Ukraine: " + ukrainePopulatedPlaceDs.filter(_.ADM4_EN == "Kyiv").count())
  //  println("Count of Lviv in Ukraine: " + ukrainePopulatedPlaceDs.filter(_.ADM4_EN == "Lviv").count())
  //  println("Count of Ivanivka in Ukraine: " + ukrainePopulatedPlaceDs.filter(_.ADM4_EN == "Ivanivka").count())

  ukrainePopulatedPlaceDs
    .groupBy(col("ADM4_EN"))
    .count()
    .orderBy(col("count").desc)
  // .show()

  val monumentsDs = monumentRepo.cleanDataset()
  //  monumentsDs.show()

  monumentsDs
    .join(
      ukrainePopulatedPlaceDs,
      col("municipality") === ukrainePopulatedPlaceDs.col("ADM4_UK")
    )
    .groupBy(col("ADM4_EN"))
    .count()
    .orderBy(col("count").desc)
  //  .show(20, truncate = false)

}
