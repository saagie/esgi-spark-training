package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkFinalProject {

  val parser = new scopt.OptionParser[Parameters](usage) {
    opt[String]("path")
      .abbr("p")
      .action((p, c) => c.copy(path = Some(p)))
      .text("Path to files")
      .required()

  def main(args: Array[String]) {


    Thread.sleep(300000)
  }

}
