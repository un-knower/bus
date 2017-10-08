package cn.sibat.truck

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * 货车数据清洗（主要是经纬度以及时间格式处理）
  * Created by wing on 2017/9/26.
  */
class TruckDataClean extends Serializable{

    /**
      * 数据格式化
      * @return RDD[TruckData]
      */
    def formatUtil(rdd: RDD[(String, String)]): RDD[TruckData] = {
        rdd.flatMap(tuple => {
            val truckDataBuffer = new ArrayBuffer[(TruckData)]
            val pattern = new Regex("[^/]+(?=.csv)") //匹配文件名（去除后缀）
            val carId = pattern.findAllIn(tuple._1).mkString(",")
            tuple._2.split("\n").foreach(records => {
                val arr = records.substring(1).split(",")
                val lon = arr(0).toDouble
                val lat = arr(1).toDouble
                val time = arr(2)
                val status = arr(3)
                val speed = arr(4).toInt
                val angle = arr(5).toInt
                truckDataBuffer.+=(TruckData(carId, lon, lat, time, status, speed, angle))
            })
            truckDataBuffer
        }).distinct()
    }

    /**
      * 计算时间差的工具
      * @param firstTime 上一个时间点
      * @param lastTime 下一个时间点
      * @return result(s)
      */
    def timeDiffUtil(firstTime: String, lastTime: String):  Double = {
            var result = -1.0
            try {
                val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) / 1000
            }catch {
                case e:Exception => e.printStackTrace()
            }
            result
    }

    /**
      * 计算两个经纬度点之间的距离(m)
      * @param lon1 第一个点的经度
      * @param lat1 第一个点的纬度
      * @param lon2 第二个点的经度
      * @param lat2 第二个点的纬度
      * @return
      */
    def distanceUtil(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
        val EARTH_RADIUS: Double = 6378137
        val radLat1: Double = lat1 * math.Pi / 180
        val radLat2: Double = lat2 * math.Pi / 180
        val a: Double = radLat1 - radLat2
        val b: Double = lon1 * math.Pi / 180 - lon2 * math.Pi / 180
        val s: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
        s * EARTH_RADIUS
    }

    /**
      * 利用前一个点和后一个点的位置和时间求位移和平均速度，并过滤漂移点（平均速度过大）和因堵车或红绿灯而导致瞬时速度为0的位置数据
      * 这里需要考虑两种OD，一种是一天只有一次OD（没有熄火地点，整天都在路上）；一种是一天有一次以上的OD（熄火地点有一个以上）
      * @param df 格式化后的dataframe
      * @return
      */
    def filterUtil(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        df.groupByKey(row => row.getString(row.fieldIndex("carId")) + row.getString(row.fieldIndex("time")).split(" ")(0)).flatMapGroups((key, iter) => {
            val result = new ArrayBuffer[String]
            var firstTime = ""
            var firstLon = 0.0
            var firstLat = 0.0
            iter.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
                //初始化第一条数据
                if (result.isEmpty) {
                    firstTime = row.getString(row.fieldIndex("time"))
                    firstLon = row.getDouble(row.fieldIndex("lon"))
                    firstLat = row.getDouble(row.fieldIndex("lat"))
                    result.+=(row.mkString(",") + ",0.0,0.0")
                } else {
                    //后面的点以平移的方式对前面的点求时间差和平移速度
                    val lastTime = row.getString(row.fieldIndex("time"))
                    val lastLon = row.getDouble(row.fieldIndex("lon"))
                    val lastLat = row.getDouble(row.fieldIndex("lat"))
                    val elapsedTime = timeDiffUtil(firstTime, lastTime)
                    val movement = distanceUtil(firstLon, firstLat, lastLon, lastLat)
                    var avgSpeed = 0.0
                    if (!elapsedTime.equals(0.0))
                        avgSpeed = movement / elapsedTime //平均速度
                    result .+=(row.mkString(",") + "," + elapsedTime + "," + avgSpeed )
                    firstTime = lastTime
                    firstLon = lastLon
                    firstLat = lastLat
                }
            })
            result
        }).map(record => {
            val arr = record.split(",")
            (arr(0), arr(1).toDouble, arr(2).toDouble, arr(3), arr(5).toInt, arr(7).toDouble, arr(8).toDouble)
        }).toDF("carId", "lon", "lat", "time", "speed", "elapsedTime", "avgSpeed")
            .filter(col("avgSpeed") < 33.33)
//            .filter(col("avgSpeed") < 120 && col("speed") =!= 0)
    }
}

object TruckDataClean{
    //语法糖
    def apply(): TruckDataClean = new TruckDataClean()

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Truck Data Processing")
            .setMaster("local[*]")
            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[TruckData]))

        val sc: SparkContext = new SparkContext(conf)
//        sc.setLogLevel("ERROR")
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val dataPath = "truckData/20161201"
        val rdd = sc.wholeTextFiles(dataPath)
        val formatData = TruckDataClean.apply().formatUtil(rdd).toDF()
        val cleanData = TruckDataClean.apply().filterUtil(formatData)
        cleanData.filter(col("elapsedTime") >= 160*60).groupByKey(row => row.getString(row.fieldIndex("carId"))).count().repartition(1).rdd.saveAsTextFile("160")
        sc.stop()
    }
}

/**
  * 货车数据
  * @param carId 车牌号
  * @param lon 经度
  * @param lat 纬度
  * @param time 定位时间
  * @param status 定位状态
  * @param speed 速度
  * @param angle 角度
  */
case class TruckData(carId: String, lon: Double, lat: Double, time: String, status: String, speed: Int, angle: Int)
