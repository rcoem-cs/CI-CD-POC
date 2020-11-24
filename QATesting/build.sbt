name := "QATesting"

version := "0.1"

scalaVersion := "2.11.12"
autoScalaLibrary := false
val sparkVersion = "2.4.6"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.6" % "provided"