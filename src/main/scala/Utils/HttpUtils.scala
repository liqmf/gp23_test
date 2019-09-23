package Utils


import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

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
  * Http请求协议 GET请求
  */
object HttpUtils {
  /**
    * GET请求
    * @param url
    * @return json
    */
  def get(url:String):String={
    //创建客户端
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //发送请求
    val httpResponse = client.execute(httpGet)
    //处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")

  }
}
