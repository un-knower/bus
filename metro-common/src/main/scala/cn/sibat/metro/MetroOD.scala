package cn.sibat.metro

import cn.sibat.metro.utils.TimeUtils

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 地铁乘客OD计算
  * Created by wing1995 on 2017/5/10.
  */
class MetroOD(df: DataFrame) extends Serializable {
  /**
    * 将清洗完的数据返回
    * @return DataFrame
    */
  def toDF: DataFrame = this.df

  /**
    * 构造伴生对象，返回对象本身，实现链式写法
    * @param df 清洗后的DataFrame
    * @return 原对象 DataClean
    */
  private def newUtils(df: DataFrame): MetroOD = new MetroOD(df)

  /**
    * 将乘客的相邻两个刷卡记录两两合并为一条字符串格式的OD记录
    *
    * @param arr 每一个乘客当天的刷卡记录组成的数组
    * @return 每一个乘客当天使用深圳通乘坐地铁产生的OD信息组成的数组
    */
  def generateOD(arr: Array[String]): Array[String] = {
    val newRecords = new ArrayBuffer[String]()
    for (i <- 1 until arr.length) {
      val emptyString = new StringBuilder()
      val OD = emptyString.append(arr(i-1)).append(',').append(arr(i)).toString()
      newRecords += OD
    }
    newRecords.toArray
  }

  /**
    * 乘客OD信息的生成
    * 根据刷卡卡号进行分组，将每位乘客的OD信息转换为数组，按刷卡时间排序，
    * 将排序好的数组转换为逗号分隔的字符串，最后将字符串两两合并生成乘客OD信息
    * @return
    */
  def calMetroOD: MetroOD = {
    import df.sparkSession.implicits._
    val mergedData = df.groupByKey(row => row.getString(row.fieldIndex("cardCode")) + ',' + row.getString(row.fieldIndex("date")))
      .flatMapGroups((_, records) => {
        val sortedArr = records.toArray.sortBy(row => row.getString(row.fieldIndex("cardTime"))).map(_.mkString(","))
        val mergedOD = generateOD(sortedArr)
        mergedOD
      })
    val filteredData = mergedData.map(_.split(",")).filter(arr => arr(2)=="21" && arr(10) == "22")
    val mergedResult = filteredData.map(arr => OD(arr(0), arr(1), arr(4), arr(5), arr(6), arr(9), arr(11).toDouble, arr(12), arr(13), arr(14), arr(15))).toDF()
    newUtils(mergedResult)
  }

  /**
    * 添加过滤条件
    * 生成进站与出站的时间差列，保留出时间差小于3小时以及出入站点不同的记录
    * @return
    */
  def filteredRule: MetroOD = {
    val timeUtils = new TimeUtils
    val timeDiffUDF = udf((startTime: String, endTime: String) => timeUtils.calTimeDiff(startTime, endTime))
    val ODsCalTimeDiff = df.withColumn("timeDiff", timeDiffUDF(col("cardTime"), col("outCardTime")))
    val timeLessThan3 = ODsCalTimeDiff.filter(col("timeDiff") < 3)
    val inNotEqualToOut = timeLessThan3.filter(col("siteName") =!= col("outSiteName"))
    newUtils(inNotEqualToOut)
  }
}

object MetroOD {
  def apply(df: DataFrame): MetroOD = new MetroOD(df)

  def main(args: Array[String]): Unit = {
    var oldDataPath = "/user/wuying/SZT_original/oldData"
    var newDataPath = "/user/wuying/SZT_original/newData"
    var staticMetroPath = "/user/wuying/metroInfo/subway_station"
    if (args.length == 3) {
      oldDataPath = args(0)
      newDataPath = args(1)
      staticMetroPath = args(2)
    } else System.out.println("使用默认的参数配置")

    val spark = SparkSession.builder()
      .appName("CarFreeDayAPP")
      .master("local[3]")
      .config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my")
      .getOrCreate()

    val oldDf = DataFormat.apply(spark).getOldData(oldDataPath)
    val newDf = DataFormat.apply(spark).getNewData(newDataPath)
    val bStationMap = DataFormat.apply(spark).getStationMap(staticMetroPath)
    val df = oldDf.union(newDf).distinct()
    val cleanDf = DataClean.apply(df).addDate().recoveryData(bStationMap).toDF
    val resultDf = MetroOD.apply(cleanDf).calMetroOD.filteredRule.toDF
    resultDf.show()
  }
}

/**
  * OD记录的基本数据结构
  * @param cardCode 刷卡卡号
  * @param cardTime 刷卡时间
  * @param terminalCode 终端编码
  * @param routeName 路径名称（公司名称）
  * @param siteName 站点名称（线路名称）
  * @param outCardTime 出站刷卡时间
  * @param tradeValue 旅程交易值
  * @param outTerminalCode 出站终端编码
  * @param outRouteName 出站线路名称
  * @param outSiteName 出站站点名称
  * @param date 日期
  */
case class OD(cardCode: String, cardTime: String, terminalCode: String, routeName: String, siteName: String,
              outCardTime: String, tradeValue: Double, outTerminalCode: String, outRouteName: String, outSiteName: String, date: String
             )
