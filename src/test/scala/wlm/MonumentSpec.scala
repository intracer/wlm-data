package wlm

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MonumentSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  val lang: Lang.Value = Lang.EN
  lazy val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
  lazy val monumentRepo = new MonumentRepo(spark, lang)

  "cleanMunicipality" should "clean wiki link [[x]] into x" in {
    Monument.cleanMunicipality("[[Kyiv]]") shouldBe "Kyiv"
  }

  "cleanMunicipality" should "clean disambiguating wiki link [[x (specification)|x]] into x" in {
    Monument.cleanMunicipality("[[Dnipro (city)|Dnipro]]") shouldBe "Dnipro"
  }

  "monumentsWithUnmappedKoatuu" should "not be empty" in {
    val monumentsWithUnmappedKoatuu = monumentRepo.monumentsWithUnmappedKoatuu()
    monumentsWithUnmappedKoatuu.groupBy("adm1", "adm2").count().collect().toSeq should not be Nil
  }

  "numberOfMonumentsByAdm" should "descending counts for all adm1 regions with some range checks" in {
    val rows = monumentRepo.numberOfMonumentsByAdm(AdmLevel.ADM1).collect()
    rows.map(_.adm).toSet shouldBe PopulatedPlaceSpec.adm1NamesWithoutKyivAndSevastopol // TODO fix
    val counts = rows.map(_.count).toSeq
    counts.reverse shouldBe sorted
    all(counts) should be > 500L
    all(counts) should be < 15000L
    counts.sum should be > 95000L
  }

  "numberOfPicturedMonumentsByAdm" should "descending counts for all adm1 regions with some range checks" in {

    val rows = monumentRepo.numberOfPicturedMonumentsByAdm(AdmLevel.ADM1).collect()
    rows.map(_.adm).toSet shouldBe PopulatedPlaceSpec.adm1NamesWithoutKyivAndSevastopol // TODO fix
    val counts = rows.map(_.count).toSeq
    counts.reverse shouldBe sorted
    all(counts) should be > 400L
    all(counts) should be < 5000L
    counts.sum should be > 35000L
  }

  "percentageOfPicturedMonumentsByAdm" should "descending counts for all adm1 regions with some range checks" in {
    def groupByAdm[T <: HasAdm](adms: Dataset[T]): Map[AdmName, T] =
      adms
        .collect()
        .groupBy(_.adm)
        .mapValues(_.head)

    val admLevel = AdmLevel.ADM1

    val ds = monumentRepo.percentageOfPicturedMonumentsByAdm(admLevel)
    ds.collect().toSeq.map(_.percentage).reverse shouldBe sorted

    val percentagesMap: Map[AdmName, PercentagePerAdm] = groupByAdm(ds)
    val picturedMap: Map[AdmName, Long] = groupByAdm(monumentRepo.numberOfPicturedMonumentsByAdm(admLevel)).mapValues(_.count)
    val totalMap: Map[AdmName, Long] = groupByAdm(monumentRepo.numberOfMonumentsByAdm(admLevel)).mapValues(_.count)
    val tolerance: Double = 0.01

    percentagesMap.keySet shouldBe PopulatedPlaceSpec.adm1Names
    percentagesMap.map {
      case (adm, stat) =>
        stat.all shouldBe totalMap(adm)
        stat.part shouldBe picturedMap(adm)
        stat.percentage shouldBe (100.0 * stat.part / stat.all +- tolerance)
    }
  }

}
