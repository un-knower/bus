package cn.sibat.metroFlowForcast

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import cn.sibat.metroUtils.{MetroOD, TimeUtils}

import scala.collection.mutable.ArrayBuffer

/**
  * 根据历史信息找出不同时间段发生大客流的站点
  * Created by wing1995 on 2017/7/13.
  */
class StationFlow(val data :DataFrame) extends Serializable{
  import this.data.sparkSession.implicits._
  /**
    * 返回数据
    * @return df
    */
  def getDF: DataFrame = this.data

  /**
    * 返回对象本身实现链式写法
    * @param df DataFrame
    * @return StationFlow对象
    */
  private def newObject(df : DataFrame): StationFlow = new StationFlow(df)

  /**
    * 清洗工具
    * @return
    */
  def cleanData(): StationFlow = {
    val cleanedDf = this.data.filter(col("transType") =!= "01")
    newObject(cleanedDf)
  }

  /**
    * 按凌晨3点作切割，将当天3点以后到次日3点的数据作为一天的地铁通行数据
    * @return
    */
  def addDate(): DataFrame = {
    val time2stamp = udf((time: String) => TimeUtils.apply.time2stamp(time.replace("T", " "), "yyyy-MM-dd HH:mm:ss.SSS") - 8 * 60 * 60)
    val addStamp = this.data.withColumn("dateStamp", time2stamp(col("cardTime")))

    val time2date = udf{(time: String) => time.split("T")(0)}
    val addDate = addStamp.withColumn("oldDate", time2date(col("cardTime"))) //旧日期

    val addBeginTime = addDate.withColumn("beginTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 3) //当天03：00
    val addEndTime = addBeginTime.withColumn("endTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 27) //次日03：00

    val addNewDate = addEndTime.withColumn("date", when(col("dateStamp") > col("beginTime") && col("dateStamp") < col("endTime"), col("oldDate"))
      .otherwise(date_format((col("dateStamp") - 60 * 60 * 24).cast("timestamp"), "yyyy-MM-dd")))
      .drop("dateStamp", "oldDate", "beginTime", "endTime")

    addNewDate
  }

  /**
    * 合并出站入站记录，生成OD
    * @return
    */
  def mergeOD(): StationFlow = {

    val metroRDD = this.data.rdd.map(records => Metro(records(0).toString, records(1).toString, records(2).toString, records(3).toString, records(4).toString))
      .map(records => (records.cardCode, records))
      .groupByKey()
      .flatMap(records => {
      val sortedArr = records._2.toArray.sortBy(_.cardTime)

      //将数组里面的每一条单独的记录连接成字符串
      val stringRecord = sortedArr.map(record => record.cardCode + ',' + record.cardTime + ',' + record.siteCode + ',' + record.transType)

      def generateOD(arr: Array[String]): Array[String] = {
        val newRecords = new ArrayBuffer[String]()
        for (i <- 1 until arr.length) {
          val emptyString = new StringBuilder()
          val OD = emptyString.append(arr(i-1)).append(',').append(arr(i)).toString()
          newRecords += OD
        }
        newRecords.toArray
      }

      generateOD(stringRecord)
    })

    val df = metroRDD.map(record => record.split(","))
      .filter(arr => arr(3) == "21" && arr(7) == "22")
      .map(arr => OD(arr(0), arr(1), arr(2), arr(5), arr(6)))
      .toDF()
      .filter(col("inSiteCode") =!= col("outSiteCode"))

    newObject(df)
  }

  /**
    * 获取任意两站之间的平均花费时间
    * @return
    */
  def getStationTime: StationFlow = {

    val timeDiffUDF = udf((startTime: String, endTime: String) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val timeDiff = (sdf.parse(endTime.replace("T", " ")).getTime - sdf.parse(startTime.replace("T", " ")).getTime) / (60F * 1000F) //得到分钟为单位的时间差
      timeDiff
    })
    val TimeDf = this.data.withColumn("timeDiff", timeDiffUDF(col("inCardTime"), col("outCardTime")))
    val avgTimeDf = TimeDf.groupBy(col("inSiteCode"), col("outSiteCode")).avg("timeDiff")
    newObject(avgTimeDf)
  }
}

object StationFlow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Metro Data Test")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
//    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\subwayData\\2017-01-03")
    val ds = spark.read.textFile(args(0))
    val df = ds.map(row => row.split(",")).map(arr => Metro(arr(0), arr(1), arr(2), arr(3), arr(4))).toDF()
    val newDF = new StationFlow(df).cleanData().addDate()
    val result = new StationFlow(newDF).mergeOD().getDF
    result.rdd.map(row => row.mkString(",")).saveAsTextFile("E:\\trafficDataAnalysis\\testData\\targetSZTOD-201706")
  }
}

/**
  * 地铁乘车数据
  * @param cardCode 卡号
  * @param cardTime 刷卡时间
  * @param siteCode 站点编码
  * @param transType 交易类型
  * @param upTime 上传时间
  */
case class Metro(cardCode: String, cardTime: String, siteCode: String, transType: String, upTime: String)

/**
  * 地铁OD数据
  * @param cardCode 卡号
  * @param inCardTime 入站刷卡时间
  * @param inSiteCode 入站站点编码
  * @param outCardTime 出站刷卡时间
  * @param outSiteCode 出站站点编码
  */
case class OD(cardCode: String, inCardTime: String, inSiteCode: String, outCardTime: String, outSiteCode: String)