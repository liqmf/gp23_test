package test

import breeze.linalg.mapValues
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{GenTraversableOnce, mutable}
import scala.collection.mutable.ListBuffer

/**
  *
  * Description:xxxx<br/>
  *
  * Copyright(c)<br/>
  *
  * All rights reserved.
  *
  * @author李海畅
  * @version:1.0
  *
  */
object exam1 {
  def main(args: Array[String]): Unit = {

    var list: List[List[String]] = List()
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val log: RDD[String] = sc.textFile("test00/json.txt")

    val logs: mutable.Buffer[String] = log.collect().toBuffer

    for (i <- 0 until  logs.length){
      val str = logs(i).toString
      val jsonparse = JSON.parseObject(str)
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""
      val regeocodejson = jsonparse.getJSONObject("regeocode")
      if (regeocodejson == null || regeocodejson.keySet().isEmpty) return ""
      val poisArray = regeocodejson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null

      val buffer = collection.mutable.ListBuffer[String]()
      for(item <- poisArray.toArray()){
        if (item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))

        }

      }
      val list1 :List[String] = buffer.toList
      list :+=list1

    }
    val res1 :List[(String,Int)] = list.flatMap(x =>x).filter(x =>x!="[]").map(x =>(x,1))
      .groupBy(x =>x._1).mapValues(x =>x.size).toList.sortBy(x => x._2)
    res1.foreach(println)
  }
}

