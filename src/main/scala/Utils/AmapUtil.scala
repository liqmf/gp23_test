package Utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.util.parsing.json.JSONArray

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

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {

  /**
    * 解析经纬度
    */


  // 获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode
    // /regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    val location = long+","+lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?key=fc0bfbfd19dc074aebfda63f6d1a8ea0&location="+location
    // 调用请求
    val jsonstr = HttpUtils.get(urlStr)
    // 解析json串
    val jsonparse = JSON.parseObject(jsonstr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }

}
