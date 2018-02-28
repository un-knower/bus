package cn.sibat.bus

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import cn.sibat.bus.utils.{DAOUtil, DateUtil, LocationUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by kong on 2017/12/6.
  */
object BusArrivalAppTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("BusArrivalApp").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    // --------------从数据库中读取静态数据--------------------------------------------------------
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
    val sql = "select l.ref_id as route,l.direction as direct,s.station_id as stationId,ss.name as stationName,s.stop_order as stationSeqId,ss.lat as stationLat,ss.lon as stationLon,l.name as lineName from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id"
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
      val lineName = row.getString(row.fieldIndex("lineName"))
      val direct = row.getString(row.fieldIndex("direct"))
      val stationId = row.getString(row.fieldIndex("stationId"))
      val stationName = row.getString(row.fieldIndex("stationName"))
      val stationSeqId = row.getInt(row.fieldIndex("stationSeqId"))
      val stationLon = row.getString(row.fieldIndex("stationLon"))
      val stationLat = row.getString(row.fieldIndex("stationLat"))
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      StationData(route, lineName, direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble, 0)
    }.collect()

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val mapCheckpoint = checkPoint.groupBy(sd => sd.lineId + "," + sd.direct)
    //广播静态数据
    val bMapStation = spark.sparkContext.broadcast(mapStation)
    val bMapCheckpoint = spark.sparkContext.broadcast(mapCheckpoint)

    //----------------到站计算-------------------------
    //1.加载数据
//    val data = spark.read.textFile("E:/busdata/STRING_20171116")
//    //2.清洗数据
//    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
//    //.upTimeFormat("yy-M-d H:m:s").filterErrorDate()
//    val filter = busDataCleanUtils.dataFormat().zeroPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().filterStatus()
    val data = spark.read.parquet("E:/data/001901")
    val filter = new BusDataCleanUtils(data)

    //3.到站计算
    val roadInformation = new RoadInformation(filter)
    val toStation = roadInformation.toStation1(bMapStation, bMapCheckpoint)
    toStation.persist(StorageLevel.MEMORY_AND_DISK)
    //toStation.write.csv("")
    //toStation.repartition(1).write.csv("E:/data/19-test-2")
    //println(toStation.select("tripId").distinct().count())

    //4. 写到mysql

    //172.16.3.200:4522 xbpeng(xbpeng) bus_roundtrip
    //id,create_date,modify_date,bus_id,line_dir,end_station_id,end_station_index,end_gps_time,est_start_time,line_id,
    // miss_station_list,real_ratio,service_date,start_station_id,start_station_index,start_gps_time,trip_timecost,
    // total_ratio,trip_mile,line_dir_id
    val df = toStation.as[BusArrivalHBase2].groupByKey(_.tripId).mapGroups((key, it) => {
      val itArr = it.toArray
      val exits = itArr.filter(bah => !bah.arrivalTime.equals("null") && !bah.leaveTime.equals("null")).sortBy(_.stationIndex)
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val startStation = exits(0)
      val endStation = exits(exits.length - 1)
      val currentDate = new Timestamp(System.currentTimeMillis())
      val trip_timecost = DateUtil.dealTime(startStation.arrivalTime, endStation.arrivalTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val miss = itArr.filter(bah => bah.arrivalTime.equals("null") && bah.leaveTime.equals("null")).map(_.stationIndex).mkString("/")
      val length = exits.length

      val carId = startStation.rowKey.split("\\|")(1)
      BusArrivalMySQL200(startStation.tripId, currentDate, currentDate, carId, startStation.direct, endStation.stationId, endStation.stationIndex.toInt, new Timestamp(sdf.parse(endStation.arrivalTime).getTime)
        , new Timestamp(sdf.parse(endStation.arrivalTime).getTime), new Timestamp(sdf.parse(startStation.arrivalTime).getTime), startStation.lineId, miss, s"$length/${startStation.total}", new Timestamp(sdf.parse(startStation.arrivalTime).getTime)
        , startStation.stationId, startStation.stationIndex.toInt, new Timestamp(sdf.parse(startStation.arrivalTime).getTime), trip_timecost.toInt, s"$length/${startStation.total}", startStation.trip_mile.toString, startStation.tripId)
    }).toDF()

    //172.16.3.205:3306 client(client) roundtrip_15lines_v2
    //id,bus,day,direction,endstation,endtime,linename,missstationlist,realration,starttime,timecost,totalratio,
    // dummyendstation,dummyendtime,dummyendstartstion,dummystarttime
    val df2 = toStation.as[BusArrivalHBase2].groupByKey(_.tripId).mapGroups((key, it) => {
      val itArr = it.toArray
      val exits = itArr.filter(bah => !bah.arrivalTime.equals("null") && !bah.leaveTime.equals("null")).sortBy(_.stationIndex)
      val startStation = exits(0)
      val endStation = exits(exits.length - 1)
      val trip_timecost = DateUtil.dealTime(startStation.arrivalTime, endStation.arrivalTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val miss = itArr.filter(bah => bah.arrivalTime.equals("null") && bah.leaveTime.equals("null")).map(_.stationIndex).mkString("/")
      val length = exits.length
      val carId = startStation.rowKey.split("\\|")(1)
      val sd = bMapStation.value.getOrElse(startStation.lineId + "," + startStation.direct, Array())
      val minSD = sd.minBy(_.stationSeqId)
      val maxSD = sd.maxBy(_.stationSeqId)
      val direct = if (startStation.direct.equals("up")) "上行" else "下行"
      val startName = sd.filter(s => s.stationId.equals(startStation.stationId))(0).stationName
      val endName = sd.filter(s => s.stationId.equals(endStation.stationId))(0).stationName
      val ea = endStation.arrivalTime.split("\\.")(0).replace("T", "")
      val sa = startStation.arrivalTime.split("\\.")(0).replace("T", "")
      //(yyyy-MM-dd HH:mm:ss)
      BusArrivalMySQL205(startStation.tripId, sa.split(" ")(0), minSD.lineName, carId, direct, minSD.stationName, maxSD.stationName, sa
        , ea, sa, ea, startName, endName, miss, s"$length/${startStation.total}", s"$length/${startStation.total}", trip_timecost.toString)
    }).toDF()

    //toStation.as[BusArrivalHBase2].filter(bah => !bah.arrivalTime.equals("null") && !bah.leaveTime.equals("null")).repartition(1).write.csv("E:/data/busArrival/2017-11-16")

    DAOUtil.write2SQL(df, "bus_roundtrip", "jdbc:mysql://172.16.3.200:3306/xbus_v4?user=xbpeng&password=xbpeng")
    //DAOUtil.write2SQL(df2, "roundtrip_15lines_v2", "jdbc:mysql://172.16.3.205:3306/xbus?user=client&password=client")
    toStation.unpersist()
  }
}
