package cn.sibat.bus

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by kong on 2017/8/30.
  */
object DataSearch {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("DataSearch").getOrCreate()
    val data = spark.read.textFile("D:/testData/公交处/data/STRING_20170830")

    //    val url = "jdbc:mysql://192.168.40.27:3306/xbus?user=test&password=test"
    //    val prop = new Properties()
    //    val lineDF = spark.read.jdbc(url, "line", prop)
    //    val lineStopDF = spark.read.jdbc(url, "line_stop", prop)
    //    val stationDF = spark.read.jdbc(url, "station", prop)
    //    lineDF.createOrReplaceTempView("line")
    //    lineStopDF.createOrReplaceTempView("line_stop")
    //    stationDF.createOrReplaceTempView("station")
    //    val sql = "select l.ref_id as route,l.direction as direct,s.station_id as stationId,ss.name as stationName,s.stop_order as stationSeqId,ss.lat as stationLat,ss.lon as stationLon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id order by l.ref_id,l.direction,s.stop_order"
    //    val checkPoint = spark.sql(sql).select("route").distinct().collect().map(row => row.getString(row.fieldIndex("route")))

    //2.清洗数据
    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    val filter = busDataCleanUtils.dataFormat().zeroPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().filterStatus()
    val dd = udf((upTime: String) => {
      upTime.contains("2017-08-30T08")
    })
    filter.data.filter(col("route") === "80010").show()//.sort(col("carId"), col("upTime")).rdd.repartition(1).saveAsTextFile("D:/testData/公交处/data/00075")
    //    filter.data.select("route").distinct().foreach(row => {
    //      val route = row.getString(row.fieldIndex("route"))
    //      if (!checkPoint.contains(route)){
    //        println(route)
    //      }
    //    })

  }
}
