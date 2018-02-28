package cn.sibat.bus.commons_math

import org.apache.commons.math3.stat.descriptive.{DescriptiveStatistics, SummaryStatistics}

/**
  * Created by kong on 2017/8/28.
  */
object Statistics {
  def main(args: Array[String]) {
    //commons-math3 提供的基本统计接口
    val stats = new DescriptiveStatistics()
    val summaryStats = new SummaryStatistics()
    for (i <- 0 to 100) {
      stats.addValue(i)
      summaryStats.addValue(i)
    }

    val mean = stats.getMean
    val s_mean = summaryStats.getMean
    val std = stats.getStandardDeviation
    val s_std = summaryStats.getStandardDeviation
    //获得百分位数
    val median = stats.getPercentile(50)
    println(mean,std,median)

  }
}
