package cn.sibat.bus

import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * model of BMWOD
  *
  * @param termId      gps's terminal id gps终端id
  * @param cardId      szt's card id 深圳通id
  * @param time        gps time gps时间
  * @param lon         longitude 经度
  * @param lat         latitude 纬度
  * @param costTime    cost time 花费时间(s)
  * @param mileage     mileage is the end and starting point of the spherical distance 起点与终点的球面距离(m)
  * @param speed       speed = mileage/costTime 速度(m/s)
  * @param eachMileage each point distance 所有点里程(m)
  * @param mode        transportation mode e.g(car,taxi,bus,metro) 出行模式
  */
case class BmwOD(termId: String, cardId: String, time: String, lon: Double, lat: Double, costTime: Int, mileage: Double, speed: Double, eachMileage: Double, mode: String)

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
      .map(s => s.replaceAll("\"", "").replaceAll(",,", ",null,")).map(line => {
      val split = line.split(",")
      val id = split(0)
      val gpsTime = split(8).replaceAll("\"", "")
      val lon = split(2).toDouble
      val lat = split(3).toDouble
      val imsi = split(4)
      val speed = split(5).toDouble
      val seqId = split(6)
      val direct = split(7)
      val systemTime = split(1).replaceAll("\"", "")
      (id, systemTime, lon, lat, imsi, speed, seqId, direct, gpsTime)
    }).toDF("id", "systemTime", "lon", "lat", "imsi", "speed", "seqId", "direct", "gpsTime").distinct()
      .select(col("id"), col("gpsTime").as("time"), col("lon"), col("lat"))
      .distinct().filter(col("id") === "869432068958573")
      .groupByKey(row => row.getString(row.fieldIndex("id")))
      .flatMapGroups((s, it) => {
        val map = new mutable.HashMap[String, String]()
        map.put("353211081241115", "086540035")
        map.put("353211081239424", "665388436")
        map.put("869432068958573", "293345165")
        map.put("353211081239564", "null")
        map.put("null", "null")
        map.put("353211082799392", "null")
        map.put("353211081239259", "null")
        map.put("353211081668622", "023041813")
        map.put("353211081239820", "328771992")
        map.put("353211081240331", "362774134")
        map.put("353211081242535", "292335926")
        map.put("353211081242287", "660941532")
        map.put("353211081239655", "684043989")
        map.put("353211081239614", "361823600")
        map.put("353211081240885", "687307709")
        map.put("353211081241370", "362756265")
        map.put("353211082961174", "667338104")
        map.put("353211081240463", "685844167")
        map.put("353211081240257", "362166709")
        map.put("353211081079911", "295587058")
        map.put("353211082958485", "329813505")
        map.put("353211081242337", "684160474")
        map.put("353211081242550", "322193400")
        map.put("353211081240422", "684993919")
        val result = new ArrayBuffer[String]()
        var firstTime = ""
        var firstLon = 0.0
        var firstLat = 0.0
        val od = new ArrayBuffer[BmwOD]()
        var dis = 0.0
        var count = 1
        val data = it.toArray
        val length = data.length
        data.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
          if (result.isEmpty) {
            firstTime = row.getString(row.fieldIndex("time"))
            firstLon = row.getDouble(row.fieldIndex("lon"))
            firstLat = row.getDouble(row.fieldIndex("lat"))
            result.+=(row.mkString(",") + ",0,0.0,0.0")
            //起点，时间，经度，纬度，花费时间，里程，速度，各点总距离
            od.+=(BmwOD(s, map.getOrElse(s, "null"), firstTime, firstLon, firstLat, 0, 0.0, 0.0, 0.0, "car"))
          } else {
            val lastTime = row.getString(row.fieldIndex("time"))
            val lastLon = row.getDouble(row.fieldIndex("lon"))
            val lastLat = row.getDouble(row.fieldIndex("lat"))
            val standTime = DateUtil.dealTime(firstTime, lastTime)
            val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
            val t = if (standTime > 0.0) standTime else 1.0
            val speed = movement / t

            if (standTime > 300 && count < length) {
              val firstOD = od(od.length - 1)
              val costTime = if (firstOD.costTime != 0) 0 else DateUtil.dealTime(firstOD.time, firstTime)
              val mileage = if (firstOD.costTime != 0) 0 else LocationUtil.distance(firstLon, firstLat, firstOD.lon, firstOD.lat)
              val speed = if (firstOD.costTime != 0) 0 else mileage / costTime
              val eachMileage = if (firstOD.costTime != 0) 0 else dis
              od.+=(BmwOD(s, map.getOrElse(s, "null"), firstTime, firstLon, firstLat, costTime.toInt, mileage, speed, eachMileage, "car"))
              od.+=(BmwOD(s, map.getOrElse(s, "null"), lastTime, lastLon, lastLat, 0, 0.0, 0.0, 0.0, "car"))
              dis = 0.0
            } else if (count == length - 1) {
              val firstOD = od(od.length - 1)
              val costTime = if (firstOD.costTime != 0) 0 else DateUtil.dealTime(firstOD.time, lastTime)
              val mileage = if (firstOD.costTime != 0) 0 else LocationUtil.distance(lastLon, lastLat, firstOD.lon, firstOD.lat)
              val speed = if (firstOD.costTime != 0) 0 else mileage / costTime
              val eachMileage = if (firstOD.costTime != 0) 0 else dis
              od.+=(BmwOD(s, map.getOrElse(s, "null"), lastTime, lastLon, lastLat, costTime.toInt, mileage, speed, eachMileage, "car"))
            } else {
              dis += movement
            }
            result.+=(row.mkString(",") + "," + standTime + "," + movement + "," + speed)
            firstTime = lastTime
            firstLon = lastLon
            firstLat = lastLat
            count += 1
          }
        })
        od
      })
      //.count()
      .toDF().repartition(1).rdd.saveAsTextFile("D:/testData/公交处/bmwOD_20170717")
    //      .map(s => {
    //      val split = s.split(",")
    //      (split(0), split(1), split(2).toDouble, split(3).toDouble, split(4).toDouble, split(5).toDouble, split(6).toDouble)
    //    }).toDF("id", "time", "lon", "lat", "interval", "movement", "speed").sort(col("time")).write.parquet("D:/testData/公交处/2017-06-30_parquet")
    //spark.read.parquet("D:/testData/公交处/2017-06-30_parquet")
  }
}
