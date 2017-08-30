package cn.sibat.metro

import java.io._
import java.sql.{DriverManager, PreparedStatement}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import utils.TimeUtils

/**
  * 每个深圳通卡用户的年度刷卡记录
  * @param cardId 卡号
  * @param count 出行趟次　
  * @param earliestOutTime 最早出门时间
  * @param frequentBusLine 最常乘坐的公交线路
  * @param frequentSubwayStation 最常去的地铁站点
  * @param largestNumberPeople 同一时间同一个站点进出站的人数
  * @param latestGoHomeTime 最晚回家的时间
  * @param mostExpensiveTrip 最昂贵的旅程花费
  * @param workingDays 加班天数
  * @param reducedCarbonEmission 碳排放（单位：Kg）
  * @param restDays 休息天数
  * @param tradeAmount 交易数额总计
  * @param stationNumNotBeen 还没有去过的地铁站点数目
  * @param awardRank 获奖等级
  */
case class CardRecord(cardId: String, count: Int, earliestOutTime: String, frequentBusLine: String,
                      frequentSubwayStation: String, largestNumberPeople: Int, latestGoHomeTime: String,
                      mostExpensiveTrip: Double, workingDays: Int, reducedCarbonEmission: Double, restDays: Int,
                      tradeAmount: Double, stationNumNotBeen: Int, awardRank: String)

/**
  * 无车日系统后台数据计算，计算深圳通乘客年度刷卡记录
  * Created by wing1995 on 2017/8/16.
  */
object CarFreeDayAPP extends Serializable{

    val stationMap = new mutable.HashMap[String, String]() //存放站点ID对应的站点名称
    val timeSiteHashMap = new mutable.HashMap[String, Int]() //由站点和时间组成key，得到相同时间相同站点打卡的人数

    private def getStationMap(spark: SparkSession, staticMetroPath: String): Unit = {

        val ds = spark.read.textFile(staticMetroPath)
        ds.foreach(line => {
            //System.out.println(line)
            val lineArr = line.split(",")
            stationMap.+=((lineArr(0), lineArr(1)))
        })

    }

    private def getData(spark: SparkSession, oldDataPath: String, newDataPath: String): DataFrame = {

        //读取GBK编码的文件之方式一
        //val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CarFreeDayAPP")
        //val sc = new SparkContext(sparkConf)
        //val rdd = sc.hadoopFile[LongWritable, Text, TextInputFormat](newDataPath, 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GB2312")).take(5).foreach(println)

        import spark.implicits._
        //读取深圳通旧数据（2016.08.01--2017.05.06），新数据（2017.05.07--2017.07.31）
        val oldData = spark.read.textFile(oldDataPath)

        val oldDf = oldData.map(_.split(","))
            .filter(recordArr => recordArr.length == 15)
            .map(recordArr => {
                val cardCode = recordArr(1)
                val cardTime = recordArr(8)
                val tradeType = recordArr(4)
                val trulyTradeValue = recordArr(6).toDouble / 100 //实际交易值
                val tradeValue = recordArr(5).toDouble / 100 //旅程花费
                val terminalCode = recordArr(2)
                val routeName = recordArr(12)
                val siteName = recordArr(13)
                val carId = recordArr(14)
                (cardCode, cardTime, tradeType, trulyTradeValue, tradeValue, terminalCode, routeName, siteName, carId)
            }).toDF("cardCode", "cardTime", "tradeType", "trulyTradeValue", "tradValue", "terminalCode", "routeName", "siteName", "carId")

        //读取GBK编码的csv文件之方式二
        val newData = spark.read
            .format("csv")
            .option("encoding", "GB2312")
            .option("nullValue", "None")
            .csv(newDataPath)

        val header = newData.first()

        val newDf = newData.filter(_ != header)
            .map(row => {
                val cardCode = row.getString(0)
                val cardTime = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(row.getString(1), "yyyyMMddHHmmss"), "yyyy-MM-dd HH:mm:ss")
                val tradeType = row.getString(2)
                val trulyTradeValue = row.getString(3).toDouble / 100 //实际交易值
                val tradeValue = row.getString(4).toDouble / 100 //旅程花费
                val terminalCode = row.getString(5)
                val routeName = row.getString(6)
                val siteName = row.getString(7)
                val carId = row.getString(8)
                (cardCode, cardTime, tradeType, trulyTradeValue, tradeValue, terminalCode, routeName, siteName, carId)
            }).toDF("cardCode", "cardTime", "tradeType", "trulyTradeValue", "tradValue", "terminalCode", "routeName", "siteName", "carId")

        val df = oldDf.union(newDf)

        df
    }

    def carFreeDay(df: DataFrame): DataFrame = {

        import df.sparkSession.implicits._

        //主要给数据添加日期（以凌晨4点为分割线）
        val cleanData = DataClean.apply(df).addDate().toDF
        //对地铁数据做补全站点处理
        val newData = cleanData.map(row => {
            val siteId = row.getString(row.fieldIndex("terminalCode"))
            val flag = siteId.matches("2[46].*")
            var siteName = row.getString(row.fieldIndex("siteName"))
            if (flag) {
                siteName = stationMap.getOrElse(siteId, siteName)
            }
            (row.getString(row.fieldIndex("cardCode")), row.getString(row.fieldIndex("cardTime")), row.getDouble(row.fieldIndex("trulyTradeValue")), row.getString(row.fieldIndex("terminalCode")), siteName, row.getString(row.fieldIndex("date")))
        })
            .toDF("cardCode", "cardTime", "trulyTradeValue", "terminalCode", "siteName", "date")
            .cache()

        //得到相同分钟和站点打卡的记录组合的map
        newData.distinct()
            .groupByKey(row => row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName")))
            .count()
            .toDF("timeSite", "count")
            .foreach(row => timeSiteHashMap.+=((row.getString(0), row.getLong(1).toInt)))

        val resultData = newData.distinct()
            .groupByKey(row => row.getString(row.fieldIndex("cardCode"))) //根据卡号分组对每一个卡号做组内计算
            .mapGroups((key, iter) => {

            val cardId = key //卡号

            val records = iter.toArray

            val busRecords = records.filter(row => row.getString(row.fieldIndex("terminalCode")).matches("2[235].*"))
            val metroRecords = records.filter(row => row.getString(row.fieldIndex("terminalCode")).matches("2[46].*"))

            val busCount = busRecords.length
            var metroCount = 0
            val metroCardTimes = metroRecords.length
            if (metroCardTimes % 2 == 0) metroCount = metroCardTimes / 2
            else metroCount = metroCardTimes / 2 + 1
            val count = busCount + metroCount //出行趟次

            var frequentBusLine = "null" //最常乘坐的公交线路
            if (busCount != 0) {
                frequentBusLine = busRecords
                    .groupBy(row => row.getString(row.fieldIndex("siteName")))
                    .mapValues(_.length)
                    .maxBy(_._2)
                    ._1
            }

            var frequentSubwayStation = "null" //最常去的地铁站点名称
            if (metroCount != 0) {
                frequentSubwayStation = metroRecords
                    .groupBy(row => row.getString(row.fieldIndex("siteName")))
                    .mapValues(_.length)
                    .maxBy(_._2)
                    ._1
            }

            var largestNumberPeople = 0 //同一时刻同一个地铁站刷卡时遇到的最多人数
            //得到该卡号对应的刷卡站点和刷卡时间组成的对
            val keyValuePairs = metroRecords.map(row => row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName")))
            //获得所有记录中刷卡站点和刷卡时间组成的对，并将其与每张卡产生的个人记录中的每一次刷卡站点和刷卡时间组成的字符串对进行匹配，相同的话就是与持卡人同一站点同一时间进出，并找出最大值
            keyValuePairs.foreach(eachKeyValuePair => {
                val meetPeople = timeSiteHashMap.getOrElse(eachKeyValuePair, 0) - 1
                if(meetPeople > largestNumberPeople) largestNumberPeople = meetPeople
            })

            var mostExpensiveTrip = 0.0 //最昂贵的旅程花费
            mostExpensiveTrip = records.maxBy(row => row.getDouble(row.fieldIndex("trulyTradeValue"))).getDouble(2)

            var earliestOutTime = "null" //最早出门时间
            //先求每一天的最早打卡时间，再求所有天中最小时间的最小时间
            earliestOutTime = records.groupBy(row => row.getString(row.fieldIndex("date")))
                .mapValues(iter => {
                    iter.minBy(row => row.getString(row.fieldIndex("cardTime")))
                })
                .minBy(map => map._2.getString(map._2.fieldIndex("cardTime")).split(" ")(1))
                ._2
                .getString(1)
                .split(" ")(1)

            var latestGoHomeTime= "null" //最晚回家时间
            latestGoHomeTime = records.groupBy(row => row.getString(row.fieldIndex("date")))
                .mapValues(iter => {
                    iter.maxBy(row => row.getString(row.fieldIndex("cardTime")))
                })
                .maxBy(map => map._2.getString(map._2.fieldIndex("cardTime")).split(" ")(1))
                ._2
                .getString(1)
                .split(" ")(1)

            var workingDays = 0 //加班天数
            var restDays = 0 //休息天数
            records.groupBy(row => row.getString(row.fieldIndex("date")))
                .foreach(group => {
                    val sortedArr = group._2.sortBy(row => row.getString(row.fieldIndex("cardTime")))
                    val earliestOutTime = sortedArr.head.getString(1).split(" ")(1)
                    val latestGoHomeTime = sortedArr.last.getString(1).split(" ")(1)
                    if (earliestOutTime <= "09:00:00" && latestGoHomeTime >= "20:00:00") workingDays += 1
                    else if (earliestOutTime >= "09:00:00" && latestGoHomeTime <= "20:00:00") restDays += 1
                })

            var reducedCarbonEmission = 0.0 //碳排放

            var tradeAmount = 0.0 //交易数额总计
            records.foreach(row => {
                tradeAmount += row.getDouble(row.fieldIndex("trulyTradeValue"))
            })

            var stationNumNotBeen = 0 //已经去过的地铁站点数目，一共166个站点
            val stationNumHasBeenMap = metroRecords
                .groupBy(row => row.getString(row.fieldIndex("siteName")))
            stationNumNotBeen = 166 - stationNumHasBeenMap.size

            var awardRank = "null" //获奖等级
            if (count <= 300) awardRank = "L1"
            else if (count > 300 && count <= 1000) awardRank = "L2"
            else if (count > 1000 && count <= 1500) awardRank = "L3"
            else if (count > 1500 && count <= 2000) awardRank = "L4"
            else awardRank = "L5"

            CardRecord(cardId, count, earliestOutTime, frequentBusLine, frequentSubwayStation,
                largestNumberPeople, latestGoHomeTime, mostExpensiveTrip, workingDays,
                reducedCarbonEmission, restDays, tradeAmount, stationNumNotBeen, awardRank)
            }).toDF()
        resultData
    }

    private def insertRow(row: Row, stmt:PreparedStatement): Unit ={

        stmt.setString(1, row.getString(0))
        stmt.setInt(2, row.getInt(1))
        stmt.setString(3, row.getString(2))
        stmt.setString(4, row.getString(3))
        stmt.setString(5, row.getString(4))
        stmt.setInt(6, row.getInt(5))
        stmt.setString(7, row.getString(6))
        stmt.setDouble(8, row.getDouble(7))
        stmt.setInt(9, row.getInt(8))
        stmt.setDouble(10, row.getDouble(9))
        stmt.setInt(11, row.getInt(10))
        stmt.setDouble(12, row.getDouble(11))
        stmt.setInt(13, row.getInt(12))
        stmt.setString(14, row.getString(13))

        stmt.executeUpdate()

    }

    private def saveToPostgres(rows: Iterator[Row]): Unit ={

        val url = "jdbc:postgresql://192.168.40.113:5432/szt_green"
        val user = "szt"
        val password = "szt123"
        val con = DriverManager.getConnection(url, user, password)

        val sqlStr = "INSERT INTO public.card_record(" +
            "card_id, count, earliest_out_time, frequent_bus_line, frequent_subway_station, " +
            "largest_number_people, latest_go_home_time, most_expensive_trip, working_days, " +
            "reduced_carbon_emission, rest_days, trade_amount, station_num_not_been, award_rank" +
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        val stmt = con.prepareStatement(sqlStr)

        rows.foreach(insertRow(_, stmt))
    }

    def main(args: Array[String]): Unit = {

        var oldDataPath = "/user/wuying/SZT_original/oldData"
        var newDataPath = "/user/wuying/SZT_original/newData"
        var staticMetroPath = "/user/wuying/metroInfo/subway_station"
        if (args.length == 3) {
            oldDataPath = args(0)
            newDataPath = args(1)
            staticMetroPath = args(2)
        } else System.out.println("使用默认的参数配置")

        //val spark = SparkSession.builder().appName("CarFreeDayAPP").getOrCreate()
        val spark = SparkSession.builder().appName("CarFreeDayAPP").master("local[4]").config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my").getOrCreate()

        getStationMap(spark, staticMetroPath)

        val df = getData(spark, oldDataPath, newDataPath)

        val result = carFreeDay(df)

        result.show(50)
        //result.repartition(5).foreachPartition(rows => saveToPostgres(rows))

        spark.stop()
    }
}