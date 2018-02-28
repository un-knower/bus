package cn.sibat.bus

import java.util.Properties

import cn.sibat.bus.utils.LocationUtil
import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/12/5.
  */
object StationCheck {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("BusArrivalApp").getOrCreate()
    import spark.implicits._
    //spark.sparkContext.setLogLevel("ERROR")
    // --------------从数据库中读取静态数据--------------------------------------------------------
    //192.168.40.27:3306/xbus?user=test&password=test,172.16.3.200:3306/xbus?user=xbpeng&password=xbpeng
    //172.16.100.10:3306/xbus_v2?user=root&password=root123
    val url = "jdbc:mysql://192.168.40.27:3306/xbus?user=test&password=test"
    val prop = new Properties()
    val lineDF = spark.read.jdbc(url, "line", prop)
    val lineStopDF = spark.read.jdbc(url, "line_stop", prop)
    val stationDF = spark.read.jdbc(url, "station", prop)
    val checkpointDF = spark.read.jdbc(url, "line_checkpoint", prop)
    lineDF.createOrReplaceTempView("line")
    lineStopDF.createOrReplaceTempView("line_stop")
    stationDF.createOrReplaceTempView("station")
    checkpointDF.createOrReplaceTempView("checkpoint")
    val sql = "select l.ref_id as route,l.direction as direct,s.station_id as stationId,ss.name as stationName,s.stop_order as stationSeqId,ss.lat as stationLat,ss.lon as stationLon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id"
    //经度,纬度,线路ID,位置,方向
    val sql1 = "select c.lon as lon,c.lat as lat,l.ref_id as lineId,c.point_order as order,l.direction as direct from checkpoint c,line l where c.line_id = l.id"
    val checkPoint = spark.sql(sql1).map { row =>
      val lineId = row.getString(row.fieldIndex("lineId"))
      val direct = row.getString(row.fieldIndex("direct"))
      val order = row.getInt(row.fieldIndex("order"))
      val stationLon = row.getString(row.fieldIndex("lon"))
      val stationLat = row.getString(row.fieldIndex("lat"))
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      LineCheckPoint(lon.toDouble, lat.toDouble, lineId, order, direct)
    }.collect()
    val station = spark.sql(sql).map { row =>
      val route = row.getString(row.fieldIndex("route"))
      val direct = row.getString(row.fieldIndex("direct"))
      val stationId = row.getString(row.fieldIndex("stationId"))
      val stationName = row.getString(row.fieldIndex("stationName"))
      val stationSeqId = row.getInt(row.fieldIndex("stationSeqId"))
      val stationLon = row.getString(row.fieldIndex("stationLon"))
      val stationLat = row.getString(row.fieldIndex("stationLat"))
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      StationData(route,"74", direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble, 0)
    }.collect()

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val mapCheckpoint = checkPoint.groupBy(sd => sd.lineId + "," + sd.direct)
    val tt = mapStation
    tt.foreach(t => {
      try {
        val route = t._1.split(",")
        val maybeCheckpointUp = mapCheckpoint.getOrElse(route(0) + ",up", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
        val maybeCheckpointDown = mapCheckpoint.getOrElse(route(0) + ",down", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
        val maybeStationUp = mapStation.getOrElse(route(0) + ",up", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))
        val maybeStationDown = mapStation.getOrElse(route(0) + ",down", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))

        var temp0 = "up"
        var temp1 = "up"
        val cd_sd = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationDown)
        val cd_su = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationUp)
        val cu_sd = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationDown)
        val cu_su = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationUp)
        println(t._1,cd_sd,cd_su,cu_sd,cu_su)
        if (cd_sd < cd_su && cd_sd > 0) {
          temp1 = "down"
        } else {
          temp1 = "up"
        }
        if (cu_sd < cu_su && cu_sd > 0) {
          temp0 = "down"
        } else {
          temp0 = "up"
        }
        println(temp0,temp1)
      } catch {
        //B8644,up没有checkpoint
        case e: Exception => println(t._1)
      }
    })
    //2150
    //2621
    //2493
    //println(mapStation.size)
  }
}
