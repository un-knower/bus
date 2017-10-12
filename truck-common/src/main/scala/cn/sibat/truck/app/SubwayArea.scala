package cn.sibat.truck.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import cn.sibat.truck.ParseShp

/**
  * 张伟林的地铁分区域代码
  * Created by wing on 2017/10/12.
  */
object SubwayArea {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("subway Data Processing")
//            .setMaster("local[*]")
//            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        var dataPath = "part-r-00000"
        var shpPath = "shpFile/guotuwei/行政区2017.shp"
        if (args.length == 2) {
            dataPath = args(0)
            shpPath = args(1)
        }
        val df = spark.read.textFile(dataPath)
        import df.sparkSession.implicits._
        val parseShp = ParseShp.apply(shpPath).readShp()
        df.map(line => {
            val arr = line.split(",")
            subway(arr(0), arr(1), arr(2), arr(3), arr(4).toDouble, arr(5).toDouble, arr(6), arr(7), arr(8), arr(9).toDouble, arr(10).toDouble)
        }).toDF()
            .withColumn("oArea", parseShp.getZoneNameUdf(col("o_lon"), col("o_lat")))
            .withColumn("dArea", parseShp.getZoneNameUdf(col("d_lon"), col("d_lat")))
            .map(_.mkString(",")).rdd.repartition(1).saveAsTextFile("subwayArea")
        spark.stop()
    }
}

case class subway(card_id: String, o_time: String, o_line: String, o_station_name: String, o_lon: Double, o_lat: Double,
                  d_time: String, d_line: String, d_station_name: String, d_lon: Double, d_lat: Double
                 )
