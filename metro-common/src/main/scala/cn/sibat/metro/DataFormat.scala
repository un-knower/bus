package cn.sibat.metro

import org.apache.spark.sql._

/**
  * DataSet => Scheme
  * spark.read.text读进来的则是Dataset数据结构，每一行都是一个元素，类型为Row。没有命名列
  * 将默认数据类型转换为业务所需的数据视图，筛选需要的字段，以及数据类型格式化
  * Created by wing1995 on 2017/5/4.
  */
class DataFormatUtils(data: Dataset[String]) {
  import data.sparkSession.implicits._
  /**
    * 默认将SZT刷卡产生的原始记录中的每一列贴上字段标签
    * @return
    */
  def transSZT: DataFrame = {
    val SZT_df= data.map(_.split(","))
      .map(line => SZT(line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble,
        line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14)))
      .toDF()
    SZT_df
  }

  /**
    * 从原始数据中删选出地铁刷卡数据，并给地铁打卡数据贴上字段标签
    * @return
    */
  def transMetroSZT: DataFrame = {
    val metro_SZT_df= data.map(_.split(","))
      .filter(line => line(2).matches("2[46].*"))  //刷卡设备终端编码以"24"或"26"开头的数据
      .map(line => MetroSZT(line(0), line(1), line(2), line(4), line(8), line(12), line(13), line(14)))
      .toDF()
    metro_SZT_df
  }

  /**
    * 从原始数据中筛选出公交刷卡数据，并给公交刷卡数据贴上对应的字段标签
    * @return
    */
  def transBusSZT: DataFrame = {
    val bus_SZT_df= data.map(_.split(","))
      .filter(line => line(2).matches("2[235].*")) //刷卡设备终端编码以“22”或“23”和“25”开头的数据
      .map(line => BusSZT(line(0), line(1), line(2), line(4), line(8), line(12), line(13), line(14)))
      .toDF()
    bus_SZT_df
  }

  /**
    * 地铁静态站点数据（站点名称和站点编号）
    * @return
    */
  def transMetroStation: DataFrame = {
    val metro_station_df= data.map(_.split(","))
      .map(line => MetroStation(line(0), line(1), line(2), line(3)))
      .toDF()
    metro_station_df
  }
}

/**
  * 深圳通卡原始数据
  * @param recordCode 记录编码
  * @param cardCode 卡片逻辑编码
  * @param terminalCode 刷卡设备终端编码
  * @param compCode 公司编码
  * @param transType 交易类型
  * @param tranAmount 交易金额
  * @param cardBalance 交易值（实际交易金额）
  * @param unknownField 未知字段
  * @param cardTime 拍卡时间
  * @param successSign 成功标识
  * @param unknownTime1 未知时间1
  * @param unknownTime2 未知时间2
  * @param compName 公司名称(线路名称)
  * @param siteName 站点名称
  * @param vehicleCode 车辆编号
  */
case class SZT(recordCode: String, cardCode: String, terminalCode: String, compCode: String, transType: String, tranAmount: Double,
               cardBalance: Double, unknownField: String, cardTime: String, successSign: String, unknownTime1: String,
               unknownTime2: String, compName: String, siteName: String, vehicleCode: String
              )

/**
  * 深圳通地铁数据
  * @param recordCode 记录编码
  * @param cardCode 卡片逻辑编码
  * @param terminalCode 终端编码
  * @param transType 交易类型
  * @param cardTime 拍卡时间
  * @param routeName 线路名称
  * @param siteName 站点名称
  * @param GateMark 闸机标识
  */
case class MetroSZT(recordCode: String, cardCode: String, terminalCode: String, transType: String,
                    cardTime: String, routeName: String, siteName: String, GateMark: String
                    )

/**
  * 深圳通公交数据
  * @param recordCode 记录编码
  * @param cardCode 卡片逻辑编码
  * @param terminalCode 终端编码
  * @param transType 交易类型
  * @param cardTime 拍卡时间
  * @param compName 公司名称
  * @param routeName 线路名称
  * @param licenseNum 车牌号
  */
case class BusSZT(recordCode: String, cardCode: String, terminalCode: String, transType: String,
                  cardTime: String, compName: String, routeName: String, licenseNum: String
                  )

/**
  * 站点静态信息表
  * @param siteId 站点Id
  * @param siteNameStatic 站点名称
  * @param routeId 路线Id
  * @param routeNameStatic 路线名称
  */
case class MetroStation(siteId: String, siteNameStatic: String, routeId: String, routeNameStatic: String)

object DataFormatUtils{
  def apply(data: Dataset[String]): DataFormatUtils = new DataFormatUtils(data)
}
