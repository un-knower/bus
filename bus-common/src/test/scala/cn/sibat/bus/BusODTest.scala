package cn.sibat.bus

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/7/24.
  */
object BusODTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("BusODTest").master("local[*]").getOrCreate()
    spark.read.parquet("D:/testData/公交处/data/busSZT20170507")
  }
}
