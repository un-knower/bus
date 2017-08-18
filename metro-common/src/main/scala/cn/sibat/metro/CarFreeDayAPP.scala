package cn.sibat.metro

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


case class AnnualRecord()

/**
  * 无车日系统后台数据计算，计算深圳通乘客年度刷卡记录
  * Created by wing1995 on 2017/8/16.
  */
object CarFreeDayAPP extends Serializable{

    def carFreeDay(data: Dataset[String], dfStation: DataFrame): Unit = {
        import data.sparkSession.implicits._
        val df = data.map(record => {
            val recordArr = record.split(",")
            val cardCode = recordArr(1)
            val cardTime = recordArr(8)
            val tradType = recordArr(4)
            val tradAmount = recordArr(5).toDouble / 100
            val tradValue = recordArr(6).toDouble / 100
            val terminalCode = recordArr(2)
            val routeName = recordArr(12)
            val siteName = recordArr(13)
            val carId = recordArr(14)
            (cardCode, cardTime, tradType, tradAmount, tradValue, terminalCode, routeName, siteName, carId)
        }).toDF("cardCode", "cardTime", "tradType", "tradAmount", "tradValue", "terminalCode", "routeName", "siteName", "carId")

        val cleanData = DataCleanUtils.apply(df).addDate().recoveryData(dfStation).toDF
        cleanData.select(col("cardCode"), col("cardTime"), col("tradType"), col("tradAmount"), col("tradValue"), col("routeName"), col("siteName"), col("carId"), col("date"))
            .distinct()
            .groupByKey(row => row.getString(row.fieldIndex("cardCode")))
            .count()
            .sort($"count(1)".desc)
            .describe("count(1)")
            .show()
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("CarFreeDayAPP").master("local[4]").config("spark.sql.warehouse.dir", "e:\\bus").getOrCreate()
        var dataPath = "hdfs:/127.0.0.1/user/wuying/SZT_original"
        var staticMetro = "hdfs://127.0.0.1/user/wuying/metroInfo/subway_station"
        var savePath = "hdfs:/127.0.0.1/user/wuying"
        if (args.length == 3) {
            dataPath = args(0)
            staticMetro = args(1)
            savePath = args(2)
        } else {
            System.out.println("请输入参数路径")
        }
        val data = spark.read.textFile(dataPath) //读取深圳通数据
        val dsStation = spark.read.textFile(staticMetro) //读取站点静态信息
        val dfStation = DataFormatUtils(dsStation).transMetroStation
        carFreeDay(data, dfStation)

        spark.stop()
    }
}
