package cn.sibat.truck

import cn.sibat.metro.truckOD.ParseShp
import org.apache.spark.{SparkConf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 计算货车OD，包括OD时长，OD距离，OD区域
  * Created by wing on 2017/10/8.
  */
class TruckOD extends Serializable{
    /**
      * 获取货车每日OD数据，OD分两种情况考虑
      * OD根据熄火时间分割。
      * 首先，根据第一条GPS记录初始化一个OD记录，若第二个点到第一个点的时间间隔少于30分钟则将第二个点合并到第一个点中求得OD距离，以此类推并获取货车出行距离和出行时长
      * 然后，当熄火时间超过30分钟切当前数据不是最后一条数据时，将最近的一次OD元素取出，将OD记录中的第一条GPS记录的经纬度数据与最后一个GPS记录作对比，求OD时间差，从而更新最后一条OD中的数据，并根据新的OD中的第一个元素新建一个OD记录
      * 最后，
      * @param cleanData 清洗后的数据
      * @return
      */
    def getOd(cleanData: DataFrame): DataFrame = {

        import cleanData.sparkSession.implicits._
        val groupedData = cleanData.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," +  row.getString(row.fieldIndex("time")).split(" ")(0))

        val odData = groupedData.flatMapGroups((key, iter) => {

            val truckOdBuffer = new ArrayBuffer[TruckOdData]()
            val carId = key.split(",")(0)
            val records = iter.toArray.sortBy(row => row.getString(row.fieldIndex("time")))

            var firstTime = "null"
            var firstLon = 0.0
            var firstLat = 0.0
            var dis = 0.0
            var count = 1
            val length = records.length
            records.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
                if (truckOdBuffer.isEmpty) {
                    //初始化第一个OD记录
                    firstLon = row.getDouble(row.fieldIndex("lon"))
                    firstLat = row.getDouble(row.fieldIndex("lat"))
                    firstTime = row.getString(row.fieldIndex("time"))
                    //起点数据：车牌号，出发经度，出发纬度，到达经度，到达纬度，出发时间，到达时间，花费时间，各点总距离
                    truckOdBuffer.+=(TruckOdData(carId, O_lon = firstLon, O_lat = firstLat, D_lon = 0.0, D_lat = 0.0, O_time = firstTime, D_time = "null", odTime = 0, odDistance = 0.0))
                } else {
                    val lastTime = row.getString(row.fieldIndex("time"))
                    val lastLon = row.getDouble(row.fieldIndex("lon"))
                    val lastLat = row.getDouble(row.fieldIndex("lat"))
                    val elapsedTime = row.getInt(row.fieldIndex("elapsedTime"))
                    val movement = row.getDouble(row.fieldIndex("movement"))
                    //od切分
                    if (elapsedTime > 30*60 && count < length) {
                        val firstOD = truckOdBuffer(truckOdBuffer.length - 1)
                        val odTime = TruckDataClean.apply().timeDiffUtil(firstOD.O_time, lastTime)
                        val odDistance = dis
                        val nowOD = firstOD.copy(carId, firstOD.O_lon, firstOD.O_lat, firstLon, firstLat, firstOD.O_time, firstTime, odTime, odDistance)
                        truckOdBuffer.remove(truckOdBuffer.length - 1)
                        truckOdBuffer.+=(nowOD) //替代原来初始化的OD
                        truckOdBuffer.+=(TruckOdData(carId, O_lon = lastLon, O_lat = lastLat, D_lon = 0.0, D_lat = 0.0, O_time = lastTime, D_time = "null", odTime = 0, odDistance = 0.0)) //并为当前点新建一个OD
                    } else if(count == length) { //最后一个行程
                        val lastOD = truckOdBuffer(truckOdBuffer.length - 1)
                        val odTime = TruckDataClean.apply().timeDiffUtil(lastOD.O_time, lastTime)
                        val odDistance = dis + movement
                        val nowOD = lastOD.copy(carId, lastOD.O_lon, lastOD.O_lat, lastLon, lastLat, lastOD.O_time, lastTime, odTime, odDistance)
                        truckOdBuffer.remove(truckOdBuffer.length - 1)
                        truckOdBuffer.+=(nowOD)
                    } else {
                        dis += movement
                    }
                    firstTime = lastTime
                    firstLon = lastLon
                    firstLat = lastLat
                }
                count += 1
            })
            truckOdBuffer
            })
        odData.sort("carId", "O_time").toDF()
    }

    /**
      * 获取每日货车OD出行区域
      * @param odData OD数据
      * @param shpFile shp文件路径（分三种区域：行政区、街道、交通小区）
      */
    def getOdArea(odData: DataFrame, shpFile: String): DataFrame = {
        val parseShp = new ParseShp(shpFile).readShp()
        import odData.sparkSession.implicits._
        odData.map(s => {
            val carId = s.getString(s.fieldIndex("carId"))
            val O_lon = s.getDouble(s.fieldIndex("O_lon"))
            val O_lat = s.getDouble(s.fieldIndex("O_lat"))
            val D_lon = s.getDouble(s.fieldIndex("D_lon"))
            val D_lat = s.getDouble(s.fieldIndex("D_lat"))
            val oArea = parseShp.getZoneName(O_lon, O_lat)
            val dArea = parseShp.getZoneName(D_lon, D_lat)
            val date = s.getString(s.fieldIndex("O_time")).split(" ")(0)
            (carId, O_lon, O_lat, D_lon, D_lat, oArea, dArea, date)
        }).toDF("carId", "O_lon", "O_lat", "D_lon", "D_lat", "oArea", "dArea", "date")
    }
}

object TruckOD{
    //语法糖
    def apply(): TruckOD= new TruckOD()

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Truck Data Processing")
//            .setMaster("local[*]")
//            .set("spark.sql.warehouse.dir", "file:///C:/path/to/my/")

        val spark = SparkSession.builder().config(conf).getOrCreate()
        var dataPath = "truckData/12yue/20161205.csv"
        var shpPath = "shpFile/guotuwei/行政区2017.shp"
        if (args.length == 2) {
            dataPath = args(0)
           shpPath = args(1)
        }
        val df = spark.read.format("csv").csv(dataPath)
        val formatData = TruckDataClean.apply().formatUtil2(df).toDF()
//        val ds= spark.read.textFile(dataPath)
//        val formatData = TruckDataClean.apply().formatUtil(ds).toDF()
        val cleanData = TruckDataClean.apply().filterUtil(formatData)
        val odData = TruckOD.apply().getOd(cleanData)

//        TruckOD.apply().getOdArea(odData, shpPath).show()
        TruckOD.apply().getOdArea(odData, shpPath).filter(col("oArea") =!= "null" || col("dArea") =!= "null").rdd.map(_.mkString(",")).repartition(1).saveAsTextFile("odArea")

        spark.stop()
    }
}

/**
  * 货车OD数据
  * @param carId 卡号
  * @param O_lon 起点经度
  * @param O_lat 起点纬度
  * @param D_lon 终点经度
  * @param D_lat 终点纬度
  * @param O_time 出发时间
  * @param D_time 到达时间
  * @param odTime 出行时长
  * @param odDistance 出行距离，是各个点的累加距离
  */
case class TruckOdData(carId: String, O_lon: Double, O_lat: Double, D_lon: Double, D_lat: Double, O_time: String, D_time: String, odTime: Int, odDistance: Double)