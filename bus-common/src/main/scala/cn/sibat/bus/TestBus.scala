package cn.sibat.bus

import java.util.UUID

import cn.sibat.bus.utils.LocationUtil
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class TestBus(id: String, num: String, or: String)

/**
  * hh
  * Created by kong on 2017/4/10.
  */
object TestBus {

  def Frechet(): Unit = {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/user/kongshaohong/spark-warehouse/").appName("BusCleanTest").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val start = System.currentTimeMillis()
    //    val data = spark.read.textFile("D:/testData/公交处/data/STRING_20170704").filter(_.split(",").length > 14)
    //    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    //    val format = busDataCleanUtils.dataFormat().filterStatus().errorPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().toDF
    val format = spark.read.parquet("/user/kongshaohong/bus/data/20170704Format/")

    val groupByKey = format.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    val station = spark.read.textFile("/user/kongshaohong/bus/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      new StationData(route, direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble)
    }.collect()
    //val bStation = spark.sparkContext.broadcast(station)

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val bMapStation = spark.sparkContext.broadcast(mapStation)

    groupByKey.flatMapGroups((s, it) => {
      val gps = it.toArray[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
      val stationMap = bMapStation.value
      val result = new ArrayBuffer[String]()
      val lon_lat = new ArrayBuffer[Point]()
      gps.foreach { row =>
        val split = row.split(",")
        val lon = split(8).toDouble
        val lat = split(9).toDouble
        val time = split(11)
        val lineId = split(4)
        lon_lat.+=(Point(lon, lat))
        println(lon_lat.length)
        val stationData = stationMap.getOrElse(lineId + ",up", Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        val frechetDis = FrechetUtils.compareGesture(lon_lat.toArray, stationData)
        result += row + "," + frechetDis
      }
      result
    }).rdd.saveAsTextFile("/user/kongshaohong/bus/frechetAll")

    val end = System.currentTimeMillis()

    println("开始程序时间:" + start + ";结束时间:" + end + ";耗时:" + (end - start) / 1000 + "s!!!")
  }


  def main(args: Array[String]): Unit = {
    val a = new ArrayBuffer[Int]()
    (0 to 10).foreach(i => a += i)
    println(a.mkString(","))
    val gg = 555
    a.remove(a.length-1)
    a += gg
    println(a.mkString(","))
  }
}
