name := "esgi-spark"

version := "1.0"

scalaVersion := "2.11.12"

val SPARK_VERSION = "2.4.0"
val TYPESAFE_CONFIG_VERSION = "1.3.3"
val SCALA_TEST_VERSION = "3.0.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION ,
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.scalatest" %% "scalatest" % SCALA_TEST_VERSION % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case "conf/application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

test in assembly := {}
parallelExecution in Test := false