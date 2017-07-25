package cn.sibat.bus

import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/18.
  */
object BaoMa {



  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("BaoMa").getOrCreate()
    import spark.implicits._
    val list = Array("086540035", "665388436", "023041813", "362774134", "392335926", "660941532", "684043989", "361823600"
      , "667338104", "685844167", "362166709", "295587058")
    val targetSZT = udf { (value: String) =>
      val cardId = value.split(",")(1)
      list.contains(cardId)
    }

    val data = spark.read.textFile("D:/testData/公交处/data/2017-07-17_01.txt")
    //      .map(s => s.replaceAll("\"", "").replaceAll(",,", ",null,")).map(line => {
    //      val split = line.split(",")
    //      val id = split(0)
    //      val gpsTime = split(8).replaceAll("\"", "")
    //      val lon = split(2).toDouble
    //      val lat = split(3).toDouble
    //      val imsi = split(4)
    //      val speed = split(5).toDouble
    //      val seqId = split(6)
    //      val direct = split(7)
    //      val systemTime = split(1).replaceAll("\"", "")
    //      (id, systemTime, lon, lat, imsi, speed, seqId, direct, gpsTime)
    //    }).toDF("id", "systemTime", "lon", "lat", "imsi", "speed", "seqId", "direct", "gpsTime")
    //      .select(col("id"), col("gpsTime").as("time"), col("lon"), col("lat"))
    //      .distinct()
    //      .filter(col("id") === "353211081239218").sort(col("time")).repartition(1).rdd.saveAsTextFile("D:/testData/公交处/218")

  }
}
