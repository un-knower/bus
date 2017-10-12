package cn.sibat.truck

import java.io.File
import java.nio.charset.Charset

import com.vividsolutions.jts.geom.{Coordinate, MultiPolygon}
import org.geotools.data.FeatureSource
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.{FeatureCollection, FeatureIterator}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * 本地OD匹配，找出区域内的起始点或者出发点，匹配OD到固定范围
  * Created by wing1995 on 2017/9/11.
  */
class ParseShp(shpPath: String) extends Serializable{

    //区域分析范围：Array(区域，区域ID或Name)
    var POLYGON:  Array[(MultiPolygon, String)] = _

    /**
      * 加载货车区域shp文件并将区域信息存储到数组POLYGON
      */
    def readShp(): ParseShp = {

        val file = new File(shpPath)
        var shpDataStore: ShapefileDataStore = null
        val pattern = new Regex("[^/]+(?=.shp)") //匹配文件名（去除后缀）
        val shpName = pattern.findAllIn(shpPath).mkString(",")
        try {
            shpDataStore = new ShapefileDataStore(file.toURL)
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
        val zoneName = new ArrayBuffer[String]()
        try {
            //将所有的polygon都放入数组中
            while (iterator.hasNext) {
                val sf = iterator.next()
                val attributeName = shpName match {
                    case "交通小区" => "WYID"
                    case "街道2017" => "JDNAME"
                    case "行政区2017" => "QUNAME"
                }
                zoneName += sf.getAttribute(attributeName).toString
                val multiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
                resultPolygon += multiPolygon
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            iterator.close()
            shpDataStore.dispose()
        }
        POLYGON = resultPolygon.toArray.zip(zoneName)
        this
    }

    /**
      * 获取经纬度所在的区域名称
      * 若都不在该shp文件的范围内则为null
      * @param lon 经度
      * @param lat 纬度
      * @return zoneId 当前经纬点所在的区域ID
      */
    def getZoneName(lon: Double, lat: Double): String = {
        var result = "null"
        val geometryFactory = JTSFactoryFinder.getGeometryFactory()
        val coord = new Coordinate(lon, lat)
        val point = geometryFactory.createPoint(coord)
        val targetPolygon = POLYGON.filter(t => t._1.contains(point)) //过滤区域外的点
        if (targetPolygon.nonEmpty) {
            result = targetPolygon(0)._2 //若该点属于货车分析区域，则将结果存储否则为null
        }
        result
    }
}

//可以直接通过调用伴生对象生成polygon
object ParseShp{
    def apply(shpPath: String): ParseShp = new ParseShp(shpPath)

    def main(args: Array[String]) {
        var shpPath = "行政区2017.shp"//不同等级下划分的区域shp文件路径
        if (args.length > 0) {
            shpPath = args(0)
        }
        val result = ParseShp(shpPath).getZoneName(113.868,22.711)
        println(result)
    }
}