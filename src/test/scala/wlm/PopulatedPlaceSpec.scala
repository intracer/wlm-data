package wlm

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wlm.PopulatedPlaceSpec.adm1Names

class PopulatedPlaceSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  lazy val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
  val lang: Lang.Value = Lang.EN
  lazy val populatedPlaceRepo = new PopulatedPlaceRepo(spark, lang)

  "PopulatedPlace" should "return ADM0 level name" in {
    populatedPlaceRepo.admNames(AdmLevel.ADM0).collect() shouldBe Array(AdmName("UA", "Ukraine"))
  }

  it should "return ADM1 level names" in {
    val adm1 = populatedPlaceRepo.admNames(AdmLevel.ADM1).collect().toSet
    adm1.size shouldBe 27
    adm1 shouldBe adm1Names
  }

  it should "return ADM4 level names" in {
    val adm4 = populatedPlaceRepo.admNames(AdmLevel.ADM4).collect().toSet
    adm4.size should be > 29000
    adm4.map(_.name) should contain allOf ("Kyiv", "Kharkiv", "Lviv", "Odesa", "Dnipro")
  }
}

object PopulatedPlaceSpec {
  val adm1Names: Set[AdmName] = Map(
    "UA01" -> "Autonomous Republic of Crimea",
    "UA05" -> "Vinnytska",
    "UA07" -> "Volynska",
    "UA12" -> "Dnipropetrovska",
    "UA14" -> "Donetska",
    "UA18" -> "Zhytomyrska",
    "UA21" -> "Zakarpatska",
    "UA23" -> "Zaporizka",
    "UA26" -> "Ivano-Frankivska",
    "UA32" -> "Kyivska",
    "UA35" -> "Kirovohradska",
    "UA44" -> "Luhanska",
    "UA46" -> "Lvivska",
    "UA48" -> "Mykolaivska",
    "UA51" -> "Odeska",
    "UA53" -> "Poltavska",
    "UA56" -> "Rivnenska",
    "UA59" -> "Sumska",
    "UA61" -> "Ternopilska",
    "UA63" -> "Kharkivska",
    "UA65" -> "Khersonska",
    "UA68" -> "Khmelnytska",
    "UA71" -> "Cherkaska",
    "UA73" -> "Chernivetska",
    "UA74" -> "Chernihivska",
    "UA80" -> "Kyiv",
    "UA85" -> "Sevastopol"
  ).map(AdmName.tupled).toSet

  val KyivAndSevastopol = Map("UA80" -> "Kyiv", "UA85" -> "Sevastopol").map(AdmName.tupled).toSet

  val adm1NamesWithoutKyivAndSevastopol  = adm1Names -- KyivAndSevastopol

}
