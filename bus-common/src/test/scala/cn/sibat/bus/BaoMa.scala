package cn.sibat.bus

import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/18.
  */
object BaoMa {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("T").getOrCreate()
    import spark.implicits._
    val list = Array("086540035", "665388436", "023041813", "362774134", "392335926", "660941532", "684043989", "361823600"
      , "667338104", "685844167", "362166709", "295587058")
    val targetSZT = udf { (value: String) =>
      val cardId = value.split(",")(1)
      list.contains(cardId)
    }
    val count = spark.read.textFile("D:/testData/公交处/data/2017-06-30_01.txt")
      .map(s=> s.replaceAll("\"","").replaceAll(",,",",null,")).write.parquet("D:/testData/公交处/dataForBmw")
//      .filter(s => s.split(",")(8).split(" ")(0).equals("2017-06-30")).map(line => {
//      val split = line.split(",")
//      val id = split(0)
//      val time = split(8).replaceAll("\"", "")
//      val lon = split(2).toDouble
//      val lat = split(3).toDouble
//      (id, time, lon, lat,1)
//    }).toDF("id", "time", "lon", "lat","num").groupBy("id", "time", "lon", "lat").count().count()
//    println(count)
//      .groupByKey(row => row.getString(row.fieldIndex("id")) + "," + row.getString(row.fieldIndex("time")))
//      .flatMapGroups((s, it) => {
//        val result = new ArrayBuffer[String]()
//        var firstTime = ""
//        var firstLon = 0.0
//        var firstLat = 0.0
//        it.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
//          if (result.isEmpty) {
//            firstTime = row.getString(row.fieldIndex("time"))
//            firstLon = row.getDouble(row.fieldIndex("lon"))
//            firstLat = row.getDouble(row.fieldIndex("lat"))
//            result.+=(row.mkString(",") + ",0,0.0,0.0")
//          } else {
//            val lastTime = row.getString(row.fieldIndex("time"))
//            val lastLon = row.getDouble(row.fieldIndex("lon"))
//            val lastLat = row.getDouble(row.fieldIndex("lat"))
//            val standTime = DateUtil.dealTime(firstTime, lastTime)
//            val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
//            val t = if(standTime > 0.0) standTime else 1.0
//            val speed = movement/t
//            result.+=(row.mkString(",") + "," + standTime + "," + movement + ","+speed)
//            firstTime = lastTime
//            firstLon = lastLon
//            firstLat = lastLat
//          }
//        })
//        result
//      }).map(s => {
//      val split = s.split(",")
//      (split(0), split(1), split(2).toDouble, split(3).toDouble, split(4).toDouble, split(5).toDouble,split(6).toDouble)
//    }).toDF("id", "time", "lon", "lat", "interval", "movement","speed").distinct().rdd.saveAsTextFile("D:/testData/公交处/2017-06-30_bmw")
    //spark.read.parquet("D:/testData/公交处/data/2017-06-30_bmw").show(1000)
  }
}
