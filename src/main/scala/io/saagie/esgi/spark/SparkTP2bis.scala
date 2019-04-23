package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object SparkTP2bis {

  val logRegex: Regex = """^(\S+) - - \[(\S+):(\S+):(\S+):(\S+) -\S+] "(\S+) (\S+) (\S+)\/\S+ (\S+) (\S+)""".r // Regex pattern to parse the log
  case class log(ip: String, date: String, hour: Int, min: Int, sec: Int, methodType: String, uri: String, protocol: String, response: Int, size: Int)


  // Function to assert if a String has integer and if not return 0 by default.
  def assertInt(variable: String): Int = {
    if (variable.forall(_.isDigit)) {
      variable.toInt
    } else {
      0
    }
  }

  def parseLog(line: String): log = {
    val logRegex(ip, date, hour, min, sec, methodType, uri, protocol, response, size) = line
    log(ip, date, hour.toInt, min.toInt, sec.toInt, methodType, uri, protocol, response.toInt, assertInt(size))
  }

  def main(args: Array[String]) {


    //Sleep to give time to browse Spark UI
    Thread.sleep(300000)
  }

}
