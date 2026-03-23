version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.6"

lazy val root = (project in file("."))
  .settings(
    name := "wlm-data",
    Test / parallelExecution := false
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_2.1.3" % Test
)