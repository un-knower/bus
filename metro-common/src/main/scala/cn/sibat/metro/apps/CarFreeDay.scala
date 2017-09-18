package cn.sibat.metro.apps

import java.io._
import java.sql.{DriverManager, PreparedStatement}

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}

import scala.language.postfixOps
import cn.sibat.metro.utils.TimeUtils
import cn.sibat.metro._

import scala.collection.mutable.ArrayBuffer

/**
  * 每个深圳通卡用户的每日刷卡记录产生的和可计算的数据
  *
  * @param cardId                卡号
  * @param count                 出行趟次　
  * @param earliestOutTime       最早出门时间
  * @param frequentBusLine       最常乘坐的公交线路
  * @param largestNumberPeople   同一时间同一个站点进出站的人数
  * @param latestGoHomeTime      最晚回家的时间
  * @param mostExpensiveTrip     最昂贵的旅程花费
  * @param workingDays           加班天数
  * @param reducedCarbonEmission 碳排放（单位：Kg）
  * @param restDays              休息天数
  * @param tradeAmount           交易数额总计
  * @param stationsHasBeen       已经去过的地铁站
  */
case class CardRecord(cardId: String, count: Int, earliestOutTime: String, frequentBusLine: String,
                      largestNumberPeople: Int, latestGoHomeTime: String,
                      mostExpensiveTrip: Double, workingDays: Int, reducedCarbonEmission: Double, restDays: Int,
                      tradeAmount: Double, stationsHasBeen: Array[String])

/**
  * 绿色出行入库数据
  * @param cardId 卡号
  * @param count 出行次数
  * @param earliestOutTime 最早的出门时间
  * @param frequentBusLine 乘坐最频繁的公交线路
  * @param frequentSubwayStation 乘坐最频繁的地铁站点
  * @param largestNumberPeople 同一时间同一站点打卡人数
  * @param latestGoHomeTime 最晚回家时间
  * @param mostExpensiveTrip 最贵的一次旅程花费
  * @param workingDays 加班天数
  * @param reducedCarbonEmission 减少的碳排放
  * @param restDays 休息天数
  * @param tradeAmount 乘车交易额总计
  * @param stationNumNotBeen 没有去过的地铁站点数目
  * @param awardRank 等级
  */
case class CardRecord1(cardId: String, count: Int, earliestOutTime: String, frequentBusLine: String, frequentSubwayStation: String,
                       largestNumberPeople: Int, latestGoHomeTime: String,
                       mostExpensiveTrip: Double, workingDays: Int, reducedCarbonEmission: Double, restDays: Int,
                       tradeAmount: Double, stationNumNotBeen: Int, awardRank: String)

/**
  * 深圳通卡记录
  *
  * @param cardCode        卡号
  * @param cardTime        刷卡时间
  * @param tradeType       交易类型
  * @param trulyTradeValue 真实交易金额
  * @param terminalCode    终端编码
  * @param routeName       线路名称
  * @param siteName        站点名称
  */
case class SztRecord(cardCode: String, cardTime: String, tradeType: String, trulyTradeValue: Double,
                     terminalCode: String, routeName: String, siteName: String)

/**
  * 时间站点聚合的组
  *
  * @param timeSite 时间站点组成的时间
  * @param count    同一站点统一时间打卡的人数
  */
case class timeSiteCount(timeSite: String, count: Int)

/**
  * 注册类，实现数据的kryo序列化，数据减少后相对传统的java序列化后的数据大小减少3-5倍
  */
class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[SztRecord])
        kryo.register(classOf[CardRecord1])
        kryo.register(classOf[timeSiteCount])
    }
}

/**
  * 无车日系统后台数据计算，计算深圳通乘客年度刷卡记录
  * Created by wing1995 on 2017/8/16.
  */
class CarFreeDay extends Serializable {

    /**
      * 获取站点ID对应站点名称的map，并广播到小表到各个节点
      *
      * @param spark           SparkSession
      * @param staticMetroPath 静态表路径
      * @return bStationMap
      */
    private def getStationMap(spark: SparkSession, staticMetroPath: String): Broadcast[Map[String, String]] = {
        import spark.implicits._
        val ds = spark.read.textFile(staticMetroPath)
        val stationMap = ds.map(line => {
            //System.out.println(line)
            val lineArr = line.split(",")
            (lineArr(0), lineArr(1))
        })
            .collect()
            .groupBy(row => row._1)
            .map(grouped => (grouped._1, grouped._2.head._2))

        val bStationMap = spark.sparkContext.broadcast(stationMap)
        bStationMap
    }

    /**
      * 获取原始数据
      *
      * @param spark       SparkSession
      * @param oldDataPath 2017年5月7日之前的数据
      * @param newDataPath 2017年5月7日以后的数据
      * @return df
      */
    def getData(spark: SparkSession, oldDataPath: String, newDataPath: String): DataFrame = {

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
                val trulyTradeValue = recordArr(6).toDouble / 100
                //实际交易值
                val terminalCode = recordArr(2)
                val routeName = recordArr(12)
                val siteName = recordArr(13)
                SztRecord(cardCode, cardTime, tradeType, trulyTradeValue, terminalCode, routeName, siteName)
            }).toDF()

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
                val trulyTradeValue = row.getString(3).toDouble / 100
                //实际交易值
                val terminalCode = row.getString(5)
                val routeName = row.getString(6)
                val siteName = row.getString(7)
                SztRecord(cardCode, cardTime, tradeType, trulyTradeValue, terminalCode, routeName, siteName)
            }).toDF()

        val df = oldDf.union(newDf).distinct()

        df
    }

    /**
      * 计算每个乘客的相关数据，版本一。直接对整个数据shuffle操作，存储到collect集合中，内存太大
      *
      * @param df          一年的数据
      * @param bStationMap 广播的站点数据
      * @return resultData
      */
    def carFreeDay(df: DataFrame, bStationMap: Broadcast[Map[String, String]]): DataFrame = {

        import df.sparkSession.implicits._

        //主要给数据添加日期（以凌晨4点为分割线）
        val cleanData = DataClean.apply(df).addDate().toDF
        //对地铁数据做补全站点处理
        val newData = cleanData.map(row => {
            val siteId = row.getString(row.fieldIndex("terminalCode")).substring(0, 6)
            val flag = siteId.matches("2[46].*")
            var siteName = row.getString(row.fieldIndex("siteName"))

            if (flag) {
                siteName = bStationMap.value.getOrElse(siteId, siteName)
            }
            (row.getString(row.fieldIndex("cardCode")), row.getString(row.fieldIndex("cardTime")), row.getDouble(row.fieldIndex("trulyTradeValue")), row.getString(row.fieldIndex("terminalCode")), siteName, row.getString(row.fieldIndex("date")))
        })
            .toDF("cardCode", "cardTime", "trulyTradeValue", "terminalCode", "siteName", "date")

        cleanData.unpersist()
        //得到相同分钟和站点打卡的记录组合的map，并广播到各个节点
        //        var timeSiteMap = newData
        //            .filter(row => row.getString(row.fieldIndex("terminalCode")).substring(0, 6).matches("2[46].*"))
        //            .map(row => (row.getString(row.fieldIndex("cardTime")).substring(0, 16) + row.getString(row.fieldIndex("siteName")), 1))
        //            .rdd
        //            .reduceByKey(_+_)
        //            .collect()
        //            .groupByKey(row => row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName")))
        //            .mapGroups((key, value) => timeSiteCount(key, value.length))
        //            .collect()

        //        val timeSiteBroadData = df.sparkSession.sparkContext.broadcast(timeSiteMap)
        //
        //        timeSiteMap = null

        val resultData = newData
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
            //            if(metroCount != 0) {
            //                val keyValuePairs = metroRecords.map(row => row.getString(row.fieldIndex("cardTime")).substring(0, 16) + row.getString(row.fieldIndex("siteName")))
            //                //获得所有记录中刷卡站点和刷卡时间组成的对，并将其与每张卡产生的个人记录中的每一次刷卡站点和刷卡时间组成的字符串对进行匹配，相同的话就是与持卡人同一站点同一时间进出，并找出最大值
            //                keyValuePairs.foreach(eachKeyValuePair => {
            //                    val meetPeople = timeSiteBroadData.value.filter(_._1.equals(eachKeyValuePair)).head._2 - 1
            ////                    val meetPeople = timeSiteBroadData.value.filter(_.timeSite.equals(eachKeyValuePair)).head.count - 1
            //                    if(meetPeople > largestNumberPeople) largestNumberPeople = meetPeople
            //                })
            //            }

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

            var latestGoHomeTime = "null" //最晚回家时间
            latestGoHomeTime = records.groupBy(row => row.getString(row.fieldIndex("date")))
                .mapValues(iter => {
                    iter.maxBy(row => row.getString(row.fieldIndex("cardTime")))
                })
                .maxBy(map => map._2.getString(map._2.fieldIndex("cardTime")).split(" ")(1))
                ._2
                .getString(1)
                .split(" ")(1)

            var workingDays = 0
            //加班天数
            var restDays = 0 //休息天数
            records.groupBy(row => row.getString(row.fieldIndex("date")))
                .foreach(group => {
                    val sortedArr = group._2.sortBy(row => row.getString(row.fieldIndex("cardTime")))
                    val earliestOutTime = sortedArr.head.getString(1).split(" ")(1)
                    val latestGoHomeTime = sortedArr.last.getString(1).split(" ")(1)
                    if (earliestOutTime <= "09:30:00" && latestGoHomeTime >= "20:00:00") workingDays += 1
                    else if (earliestOutTime >= "09:00:00" && latestGoHomeTime <= "18:00:00") restDays += 1
                })

            var reducedCarbonEmission = 0.0 //地铁相对私家车减少的碳排放量，根据每人每次出行地铁的碳排放是为0.06kg，私家车每10公里排放3kg
            reducedCarbonEmission = metroCount * (3 - 0.06)

            var tradeAmount = 0.0 //交易数额总计
            records.foreach(row => {
                tradeAmount += row.getDouble(row.fieldIndex("trulyTradeValue"))
            })

            var stationNumNotBeen = 0
            //已经去过的地铁站点数目，一共166个站点
            val stationNumHasBeenMap = metroRecords
                .groupBy(row => row.getString(row.fieldIndex("siteName")))
            stationNumNotBeen = 166 - stationNumHasBeenMap.size

            var awardRank = "null" //获奖等级
            if (count <= 300) awardRank = "L1"
            else if (count > 300 && count <= 1000) awardRank = "L2"
            else if (count > 1000 && count <= 1500) awardRank = "L3"
            else if (count > 1500 && count <= 2000) awardRank = "L4"
            else awardRank = "L5"

            CardRecord1(cardId, count, earliestOutTime, frequentBusLine, frequentSubwayStation,
                largestNumberPeople, latestGoHomeTime, mostExpensiveTrip, workingDays,
                reducedCarbonEmission, restDays, tradeAmount, stationNumNotBeen, awardRank)
        }).toDF()
        resultData
    }

    def calCarFree(df: DataFrame, bStationMap: Broadcast[Map[String, String]]): DataFrame = {

        import df.sparkSession.implicits._

        val cleanData = DataClean.apply(df).addDate().recoveryData(bStationMap).toDF

        val resultData = cleanData.groupByKey(row => row.getString(row.fieldIndex("date")))
            .flatMapGroups((_, iter) => {

                val cardRecordBuffer = new ArrayBuffer[CardRecord]()

                val oneDayRecords = iter.toArray

                var timeSiteMap: Iterable[timeSiteCount] = null
                if (oneDayRecords.exists(row => row.getString(row.fieldIndex("terminalCode")).substring(0, 6).matches("2[46].*"))) {
                    timeSiteMap = oneDayRecords.filter(row => row.getString(row.fieldIndex("terminalCode")).substring(0, 6).matches("2[46].*"))
                        .map(row => (row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName")), 1))
                        .groupBy(tuple => tuple._1)
                        .map(grouped => timeSiteCount(grouped._1, grouped._2.length))
                    //                    .groupBy(row => row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName")))
                    //                    .map(tuple => timeSiteCount(tuple._1, tuple._2.length))
                }

                oneDayRecords.groupBy(row => row.getString(row.fieldIndex("cardCode")))
                    .foreach(grouped => {
                        val cardCode = grouped._1
                        val records = grouped._2

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

                        var largestNumberPeople = 0 //同一站点同一时间打卡的人数
                        //得到该卡号对应的刷卡站点和刷卡时间组成的对
                        if (metroCardTimes != 0) {
                            metroRecords.foreach(row => {
                                val timeSite = row.getString(row.fieldIndex("cardTime")).substring(0, 16) + "," + row.getString(row.fieldIndex("siteName"))
                                val meetPeople = timeSiteMap.filter(_.timeSite == timeSite).head.count - 1
                                if (meetPeople > largestNumberPeople) largestNumberPeople = meetPeople
                            })
                        }

                        var mostExpensiveTrip = 0.0 //最昂贵的旅程花费
                        mostExpensiveTrip = records.maxBy(row => row.getDouble(row.fieldIndex("trulyTradeValue"))).getDouble(2)

                        val sortedArr = records.sortBy(row => row.getString(row.fieldIndex("cardTime")))
                        val earliestOutTime = sortedArr.head.getString(1).split(" ")(1)
                        //最早出门时间
                        val latestGoHomeTime = sortedArr.last.getString(1).split(" ")(1) //最晚回家时间

                        var workingDays = 0
                        //加班天数
                        var restDays = 0 //休息天数

                        if (earliestOutTime <= "09:30:00" && latestGoHomeTime >= "20:00:00") workingDays += 1
                        else if (earliestOutTime >= "09:00:00" && latestGoHomeTime <= "18:00:00") restDays += 1

                        var reducedCarbonEmission = 0.0 //碳排放

                        var tradeAmount = 0.0 //交易数额总计
                        records.foreach(row => {
                            tradeAmount += row.getDouble(row.fieldIndex("trulyTradeValue"))
                        })

                        var stationsHasBeenBuffer = new ArrayBuffer[String]
                        if (metroRecords.nonEmpty) {
                            metroRecords.groupBy(row => row.getString(row.fieldIndex("siteName")))
                                .keys
                                .foreach(stationsHasBeenBuffer.+=(_))
                        }
                        val stationsHasBeen = stationsHasBeenBuffer.toArray

                        cardRecordBuffer.+=(CardRecord(cardCode, count, earliestOutTime, frequentBusLine,
                            largestNumberPeople, latestGoHomeTime, mostExpensiveTrip, workingDays,
                            reducedCarbonEmission, restDays, tradeAmount, stationsHasBeen))
                    })
                cardRecordBuffer
            }).groupByKey(dayCardRecord => dayCardRecord.cardId)
            .mapGroups((cardCode, onePeopleRecord) => {
                val cardId = cardCode

                val records = onePeopleRecord.toArray

                var countAll = 0
                var earliestOutTimeAll = "23:59:59"

                val frequentBusLineArr = new ArrayBuffer[String] //存储各种隐射

                var frequentBusLineAll = "null"
                var frequentSubwayStationAll = "null"
                var largestNumberPeopleAll = 0
                var latestGoHomeTimeAll = "00:00:00"
                var mostExpensiveTripAll = 0.0
                var workingDaysAll = 0
                var reducedCarbonEmissionAll = 0.0
                var restDaysAll = 0
                var tradeAmountAll = 0.0

                var stationNumNotBeenAll = 0
                val subwayStationArr = new ArrayBuffer[String]

                records.foreach(cardRecord => {
                    countAll += cardRecord.count
                    if (cardRecord.earliestOutTime < earliestOutTimeAll) earliestOutTimeAll = cardRecord.earliestOutTime
                    frequentBusLineArr.+=(cardRecord.frequentBusLine)
                    if (cardRecord.largestNumberPeople > largestNumberPeopleAll) largestNumberPeopleAll = cardRecord.largestNumberPeople
                    if (cardRecord.latestGoHomeTime > latestGoHomeTimeAll) latestGoHomeTimeAll = cardRecord.latestGoHomeTime
                    if (cardRecord.mostExpensiveTrip > mostExpensiveTripAll) mostExpensiveTripAll = cardRecord.mostExpensiveTrip
                    workingDaysAll += cardRecord.workingDays
                    reducedCarbonEmissionAll += cardRecord.reducedCarbonEmission
                    restDaysAll += cardRecord.restDays
                    tradeAmountAll += cardRecord.tradeAmount
                    cardRecord.stationsHasBeen.foreach(station => subwayStationArr.+=(station))
                })

                if (frequentBusLineArr.nonEmpty) frequentBusLineAll = frequentBusLineArr.map((_, 1)).groupBy(_._1).maxBy(_._2.length)._1
                if (subwayStationArr.nonEmpty) frequentSubwayStationAll = subwayStationArr.map((_, 1)).groupBy(_._1).maxBy(_._2.length)._1
                stationNumNotBeenAll = 166 - subwayStationArr.map((_, 1)).groupBy(_._1).size

                var awardRank = "null" //获奖等级
                if (countAll <= 300) awardRank = "L1"
                else if (countAll > 300 && countAll <= 1000) awardRank = "L2"
                else if (countAll > 1000 && countAll <= 1500) awardRank = "L3"
                else if (countAll > 1500 && countAll <= 2000) awardRank = "L4"
                else awardRank = "L5"

                CardRecord1(cardId, countAll, earliestOutTimeAll, frequentBusLineAll, frequentSubwayStationAll,
                    largestNumberPeopleAll, latestGoHomeTimeAll, mostExpensiveTripAll, workingDaysAll,
                    reducedCarbonEmissionAll, restDaysAll, tradeAmountAll, stationNumNotBeenAll, awardRank)
            }).toDF()
        resultData
    }

    /**
      * 插入行
      *
      * @param row  Row
      * @param stmt PreparedStatement
      */
    private def insertRow(row: Row, stmt: PreparedStatement): Unit = {
        try {
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
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }

    /**
      * 保存数据到postgres
      *
      * @param rows 记录
      */
    private def saveToPostgres(rows: Iterator[Row]): Unit = {

        val url = "jdbc:postgresql://192.168.40.113:5432/szt_green"
        val user = "szt"
        val password = "szt123"
        val con = DriverManager.getConnection(url, user, password)

        val sqlStr = "INSERT INTO public.card_record (" +
            "card_id, count, earliest_out_time, frequent_bus_line, frequent_subway_station, " +
            "largest_number_people, latest_go_home_time, most_expensive_trip, working_days, " +
            "reduced_carbon_emission, rest_days, trade_amount, station_num_not_been, award_rank" +
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        val stmt = con.prepareStatement(sqlStr)

        rows.foreach(insertRow(_, stmt))
    }
}

object CarFreeDay {

    def apply: CarFreeDay = new CarFreeDay()

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
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "cn.sibat.metro.MyRegistrator")
            .config("spark.rdd.compress", "true")
            .getOrCreate()

        val bStationMap = CarFreeDay.apply.getStationMap(spark, staticMetroPath)

        val df = CarFreeDay.apply.getData(spark, oldDataPath, newDataPath)

        //        val result = CarFreeDay.apply.calCarFree(df, bStationMap)
        val result = CarFreeDay.apply.carFreeDay(df, bStationMap)

        result.repartition(1000).foreachPartition(rows => CarFreeDay.apply.saveToPostgres(rows))

        spark.stop()
    }
}