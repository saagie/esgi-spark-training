package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

object SparkTP2bis {

  case class log(ip:String,client:String,useriId:String,datetime:String,method:String,endpoint:String,protocol:String,responseCode:String,contentSize:Int);
  val logRegex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r


  def parseLog(line: String):log = {
    val logRegex(ip,client,userId,datetime,method,endpoint,protocol,responseCode,contentSize) = line
    return log(ip,client,userId,datetime,method,endpoint,protocol,responseCode,contentSize.toInt)
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()


    val logs = spark.sparkContext.textFile("/tmp/tp2-logs.txt",20)
    val parsedLogs = logs.flatMap(l => l.split(" "))
    parsedLogs.foreach(println(_))

    val logss = logs.filter(line=>line.matches(logRegex.toString)).map(line=>parseLog(line));
    logss.foreach(println(_))


    //Sleep to give time to browse Spark UI
    Thread.sleep(300000)
  }

}
