package cn.sibat.bus

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MissionStationCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BusArrivalApp").master("local[*]").getOrCreate()
    import spark.implicits._
    val url = "jdbc:mysql://172.16.3.200:3306/xbus_v4?user=xbpeng&password=xbpeng"
    val prop = new Properties()
    val lineDF = spark.read.jdbc(url, "NewBus_roundtrip", prop)
    lineDF.createOrReplaceTempView("roundtrip")
    val sql = "select miss_station_list,line_id,bus_id from roundtrip limit 1000"
    val sql2 = "select * from roundtrip limit 1000"

    spark.sql(sql2).show()

    val missLength = udf((miss:String) => {
      miss.split("/").length
    })

    spark.sql(sql).withColumn("missLength",col("select miss_station_list")).orderBy(col("missLength").desc).show()
  }
}
