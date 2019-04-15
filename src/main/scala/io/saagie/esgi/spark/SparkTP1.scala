package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

object SparkTP1 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val data = 1 to 100 toArray

    val rdd = spark.sparkContext.parallelize(data)

    println(s"RDD has ${rdd.partitions.length} partitions")
    rdd.foreachPartition(
      partition => println(s"Partition of length ${partition.length}")
    )

    //Sleep to give time to browse Spark UI
    Thread.sleep(300000)
  }


}
