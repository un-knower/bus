package cn.sibat.metro.truckOD

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 计算货车年报数据
  * Created by wing on 2017/9/25.
  */
object getTravelVolume {

    /**
      * 把货车OD数据的起点和终点添加对应的区域名称或ID
      *
      * @param metadata OD数据
      */
    def withZone(metadata: Dataset[String], shpFile: String, savePath: String): Unit = {
        ParseShp.read(shpFile)
        import metadata.sparkSession.implicits._
        metadata.map(s => {
            val split = s.split(",")
            val o_lon = split(1).toDouble
            val o_lat = split(2).toDouble
            val d_lon = split(3).toDouble
            val d_lat = split(4).toDouble
            val oArea = ParseShp.getZoneName(o_lon, o_lat)
            val dArea = ParseShp.getZoneName(d_lon, d_lat)
            s + "," + oArea + "," + dArea
        }).rdd.sortBy(s => s.split(",")(1) + "," + s.split(",")(2)).repartition(1).saveAsTextFile(savePath)
    }

    def main(args: Array[String]) {

        val dataPath = args(0)
        val shpPath = args(1)

        val spark = SparkSession.builder()
            .appName("TruckApp")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my")
            .getOrCreate()

        val metadata = spark.read.textFile(dataPath)
        val savePath = "E:\\货车OD\\travelVolume"

        withZone(metadata, shpPath, savePath)
    }
}

case class TruckOD(cardId: String, departLon: Double, departLat: Double, arrivalLon: Double, arrivalLat: Double)
