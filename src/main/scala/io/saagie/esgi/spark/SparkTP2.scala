package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

object SparkTP2 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val text = spark.sparkContext.textFile("data/spark-wikipedia.txt")

    val lineLengths = text
      .map(s => s.length)

    val totalLength = lineLengths.reduce((a, b) => a + b)

    println(totalLength)

    //TODO : Write your code here

    //Sleep to give time to browse Spark UI
    Thread.sleep(300000)
  }

}
