package cn.sibat.metro.truckOD

import java.io.File
import java.nio.charset.Charset

import com.vividsolutions.jts.geom.{Coordinate, MultiPolygon}
import org.apache.spark.sql.{Dataset, SparkSession}

import org.geotools.data.FeatureSource
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.{FeatureCollection, FeatureIterator}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

/**
  * 卡车的OD匹配，找出区域内的起始点或者出发点，匹配OD到固定范围
  * Created by wing1995 on 2017/9/11.
  */
object MatchTruckToShp {

    //货车数据分析范围
    private var POLYGON: Array[(MultiPolygon, String)] = _

    /**
      * 加载货车区域shp文件
      *
      * @param path 文件路径
      */
    def read(path: String): Unit = {
        val file = new File(path)
        var shpDataStore: ShapefileDataStore = null
        try {
            shpDataStore = new ShapefileDataStore(file.toURI.toURL)
            shpDataStore.setCharset(Charset.forName("GBK"))
        }
        catch {
            case e: Exception => e.printStackTrace()
        }
        val typeName = shpDataStore.getTypeNames()(0)
        var featureSource: FeatureSource[SimpleFeatureType, SimpleFeature] = null
        try {
            featureSource = shpDataStore.getFeatureSource(typeName)
        }
        catch {
            case e: Exception => e.printStackTrace()
        }
        var result: FeatureCollection[SimpleFeatureType, SimpleFeature] = null
        try {
            result = featureSource.getFeatures
        }
        catch {
            case e: Exception => e.printStackTrace()
        }
        val iterator: FeatureIterator[SimpleFeature] = result.features
        val resultPolygon = new ArrayBuffer[MultiPolygon]()
        val id = new ArrayBuffer[String]()
        try {
            //将所有的polygon都放入数组中
            while (iterator.hasNext) {
                val sf = iterator.next()
                id += sf.getID
                val multiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
                resultPolygon += multiPolygon
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            iterator.close()
            shpDataStore.dispose()
        }
        POLYGON = resultPolygon.toArray.zip(id.toIterable)
    }

    /**
      * 过去经纬度所在的交通小区的zoneId
      * 超出深圳为null
      *
      * @param lon 经度
      * @param lat 纬度
      * @return zoneId
      */
    def zoneId(lon: Double, lat: Double): String = {
        var result = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val targetPolygon = POLYGON.filter(t => t._1.contains(point)) //过滤区域外的点
        if (!targetPolygon.isEmpty) {
            result = targetPolygon(0)._2 //若该点属于货车分析区域，则将结果存储否则为null
        }
        result
    }

    /**
      * 把OD数据的起点和终点转化成交通小区编号
      *
      * @param metadata OD数据
      */
    def withZoneId(metadata: Dataset[String], savePath: String): Unit = {
        read("E:\\货车OD\\货车分析范围\\货车分析范围.shp")
        import metadata.sparkSession.implicits._
        metadata.map(s => {
            val split = s.split(",")
            val o_lon = split(1).toDouble
            val o_lat = split(2).toDouble
            val d_lon = split(3).toDouble
            val d_lat = split(4).toDouble
            val o_type = zoneId(o_lon, o_lat)
            val d_type = zoneId(d_lon, d_lat)
            s + "," + o_type + "," + d_type
        }).rdd.sortBy(s => s.split(",")(1) + "," + s.split(",")(2)).repartition(1).saveAsTextFile(savePath)
    }

    def main(args: Array[String]) {

        val spark = SparkSession.builder().appName("TruckApp").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:\\path\\to\\my").getOrCreate()
        val metadata = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\truckData\\part-r-00000")
        val savePath = "E:\\货车OD\\tables"
        withZoneId(metadata, savePath)
//        read("E:\\货车OD\\货车分析范围\\货车分析范围.shp")
//        val result = zoneId(113.88291410519841,22.499375871322698)
//        POLYGON.foreach(println)
//        println(result)
    }
}

case class TruckOD(cardId: String, departLon: Double, departLat: Double, arrivalLon: Double, arrivalLat: Double)
