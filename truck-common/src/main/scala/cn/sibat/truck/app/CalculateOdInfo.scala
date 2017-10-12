package cn.sibat.truck.app

import cn.sibat.truck.{ParseShp, TruckDataClean, TruckOD}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * OD信息量统计：出行量、发生量和吸引量、出行距离
  * Created by wing on 2017/10/11.
  */
object CalculateOdInfo {
    /**
      * 得到货车所有经过的区域
      * @param cleanData 清洗后的经纬度数据
      * @param shpFile shp
      * @return
      */
    def dataWithArea(cleanData: DataFrame, shpFile: String): DataFrame = {
        val parseShp = new ParseShp(shpFile).readShp()
        cleanData.withColumn("area", parseShp.getZoneNameUdf(col("lon"), col("lat"))).filter(col("area")=!="null")
    }

    /**
      * 区域发生的出行量统计和出行距离统计求和
      * 只要该车辆连续出现在该区域就做一次统计
      * 因此，先根据车牌号和经过的区域以及日期做聚合
      * @param cleanDataWithArea 货车经纬度数据
      * @return
      */
    def getVolume(cleanDataWithArea: DataFrame): DataFrame = {
        import cleanDataWithArea.sparkSession.implicits._
        val volumeArray = new ArrayBuffer[String]
        cleanDataWithArea.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("date")))
            .flatMapGroups((key, iter) => {
                val carId = key.split(",")(0)
                var firstArea = "null"
                var secondArea = "null"
                val areaVolume = new mutable.HashMap[String, (Double, Int)]()
                var result = new ArrayBuffer[mutable.HashMap[String, (Double, Int)]]
                val records = iter.toArray.sortBy(row => row.getString(row.fieldIndex("time")))
                records.foreach(row => {
                    if (result.isEmpty) {
                        firstArea = row.getString(row.fieldIndex("area"))
                        areaVolume.put(firstArea, (0.0, 1))
                        result.+=(areaVolume)
                    } else{
                        secondArea = row.getString(row.fieldIndex("area"))
                        if (firstArea.equals(secondArea)) {
                            val data = areaVolume.getOrElse(firstArea, (0.0, 1))
                            val areaDistance = data._1 + row.getDouble(row.fieldIndex("movement"))
                            val areaTimes = data._2 + 1
                            areaVolume.update(firstArea, (areaDistance, areaTimes))
                        } else {
                            areaVolume.put(secondArea,(0.0, 1))
                        }
                    }
                    firstArea = secondArea
                })
                areaVolume.foreach(s => {
                    val area = s._1
                    val areaDistance = s._2._1
                    val areTimes = s._2._2
                    volumeArray.+(Array(carId, area, areaDistance, areTimes).mkString(","))
                })
                volumeArray
            }).map(s => {
            val Array(carId, area, areaDistance, areaTimes) = s.split(",")
            (carId, area, areaDistance, areaTimes)
        }).toDF("carId", "area", "areaDistance", "areaTimes")
    }

    /**
      *
      * @param odData OD数据
      * @param shpFile shp文件路径
      * @return
      */
    def odWithArea(odData: DataFrame, shpFile: String): DataFrame = {
        val parseShp = new ParseShp(shpFile).readShp()
        odData.withColumn("oArea", parseShp.getZoneNameUdf(col("oLon"), col("oLat")))
            .withColumn("dArea", parseShp.getZoneNameUdf(col("dLon"), col("dLat")))
            .filter(col("oArea")=!="null" || col("dArea") =!= "null")
    }

    /**
      * 平均每天每个区域发生量
      * @param odDfWithArea 货车OD数据
      * @return avgVolume
      */
    def getOVolume(odDfWithArea: DataFrame): DataFrame = {
        val eachDayDVolume = odDfWithArea.filter(col("oArea") =!= "null").groupBy("oArea", "date").count().toDF("oArea", "date", "count")
        val avgOVolume = eachDayDVolume.groupBy("oArea", "date").avg("count")
        avgOVolume
    }

    /**
      * 平均每一天每一个区域吸引量
      * @param odDfWithArea 货车OD数据
      * @return avgDVolume
      */
    def getDVolume(odDfWithArea: DataFrame): DataFrame = {
        val eachDayDVolume = odDfWithArea.filter(col("dArea") =!= "null").groupBy("dArea", "date").count().toDF("dArea", "date", "count")
        val avgDVolume = eachDayDVolume.groupBy("dArea", "date").avg("count")
        avgDVolume
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Truck Data Processing")
            .setMaster("local[*]")
            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        var dataPath = "truckData/12yue/20161205.csv"
        var shpPath = "shpFile/guotuwei/行政区2017.shp"
        if (args.length == 2) {
            dataPath = args(0)
            shpPath = args(1)
        }
        val df = spark.read.format("csv").csv(dataPath)
        val formatData = TruckDataClean.apply().formatUtil(df).toDF()
        val cleanData = TruckDataClean.apply().filterUtil(formatData)
        println(cleanData.groupBy("carId").count().count())

//        val dataWithArea = CalculateOdInfo.dataWithArea(cleanData, shpPath)
//        CalculateOdInfo.getVolume(dataWithArea).show()

//        val odData = TruckOD().getOd(cleanData)
//        val odDataWithArea = CalculateOdInfo.odWithArea(odData, shpPath)
//        CalculateOdInfo.getDVolume(odDataWithArea).show()

        spark.stop()
    }
}
