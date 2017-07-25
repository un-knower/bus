package cn.sibat.bus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by kong on 2017/7/18.
  */
object BMWAPP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("BMWApp").getOrCreate()
    val list = Array("086540035", "665388436", "023041813", "362774134", "392335926", "660941532", "684043989", "361823600"
      , "667338104", "685844167", "362166709", "295587058")
    val targetSZT = udf { (value: String) =>
      val cardId = value.split(",")(0)
      list.contains(cardId)
    }
    spark.read.textFile("D:/testData/公交处/data/2017-06-30_01.txt").distinct().rdd.saveAsTextFile("D:/testData/公交处/data/2017-06-30_01-bmw")
    //spark.read.textFile("D:/testData/公交处/data/metroOD/*/*").filter(targetSZT(col("value"))).repartition(1).rdd.saveAsTextFile("D:/testData/公交处/targetMetroOD")
  }
}
