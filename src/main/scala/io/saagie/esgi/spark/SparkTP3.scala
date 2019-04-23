package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkTP3 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val df_baby_names = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/baby-names.csv")


    println(df_baby_names.filter($"city" === "Queens").count())


    //TODO : Print the total number of girls born in 2016

    //TODO : Print the 5 most popular name of all times

    //TODO : Print, for each year, the number of times the First Name DAVID has been given in the state of New York

    //TODO : After the join, we're losing a lot of lines because county names don't match, how can you make sure more lines can join together?

    //Sleep to give time to browse Spark UI
    Thread.sleep(300000)
  }

}
