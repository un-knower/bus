//package cn.sibat.metro.utils
//
//import java.io.File
//import java.util
//
//import org.geotools.data.DataStoreFinder
//
///**
//  * Created by wing1995 on 2017/9/11.
//  */
//object ReadShp extends App{
//    val file = new File("mayshapefile.shp")
//
//    try {
//        val connect = new util.HashMap()
//        connect.put("url", file.toURL())
//
//        val dataStore = DataStoreFinder.getDataStore(connect)
//        val typeNames = dataStore.getTypeNames();
//        val typeName = typeNames(0);
//
//        System.out.println("Reading content " + typeName);
//
//        FeatureSource featureSource = dataStore.getFeatureSource(typeName);
//        FeatureCollection collection = featureSource.getFeatures();
//        FeatureIterator iterator = collection.features();
//
//
//        try {
//            while (iterator.hasNext()) {
//                Feature feature = iterator.next();
//                Geometry sourceGeometry = feature.getDefaultGeometry();
//            }
//        } finally {
//            iterator.close();
//        }
//
//    } catch (Throwable e) {}
//}
