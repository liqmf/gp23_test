package Utils

import org.apache.spark.sql.types._


object SchemaUtils {

  val structType = StructType(
    Seq(
      StructField("sessionid", StringType),
      StructField("advertisersid", IntegerType),
      StructField("adorderid", IntegerType),
      StructField("adcreativeid", IntegerType),
      StructField("adplatformproviderid", IntegerType),
      StructField("sdkversion", StringType),
      StructField("adplatformkey", StringType),
      StructField("putinmodeltype", IntegerType),//针对广告主的投放模式,1：展示量投放 2：点击量投放
      StructField("requestmode", IntegerType),//数据请求方式（1:请求、2:展示、3:点击）
      StructField("adprice", DoubleType),//广告价格
      StructField("adppprice", DoubleType),//平台商价格
      StructField("requestdate", StringType),
      StructField("ip", StringType),
      StructField("appid", StringType),
      StructField("appname", StringType),
      StructField("uuid", StringType),
      StructField("device", StringType),
      StructField("client", IntegerType),
      StructField("osversion", StringType),
      StructField("density", StringType),
      StructField("pw", IntegerType),
      StructField("ph", IntegerType),
      StructField("long", StringType),
      StructField("lat", StringType),
      StructField("provincename", StringType),
      StructField("cityname", StringType),
      StructField("ispid", IntegerType),
      StructField("ispname", StringType),
      StructField("networkmannerid", IntegerType),
      StructField("networkmannername", StringType),
      StructField("iseffective", IntegerType),//有效标识（有效指可以正常计费的）(0：无效 1：有效
      StructField("isbilling", IntegerType),
      StructField("adspacetype", IntegerType),
      StructField("adspacetypename", StringType),
      StructField("devicetype", IntegerType),
      StructField("processnode", IntegerType),
      StructField("apptype", IntegerType),
      StructField("district", StringType),
      StructField("paymode", IntegerType),//针对平台商的支付模式，1：展示量投放(CPM) 2：点击
      StructField("isbid", IntegerType),
      StructField("bidprice", DoubleType),//竞价价格
      StructField("winprice", DoubleType),//竞价成功价格
      StructField("iswin", IntegerType),//是否竞价成功
      StructField("cur", StringType),
      StructField("rate", DoubleType),
      StructField("cnywinprice", DoubleType),//竞价成功转换成人民币的价格
      StructField("imei", StringType),
      StructField("mac", StringType),
      StructField("idfa", StringType),
      StructField("openudid", StringType),
      StructField("androidid", StringType),
      StructField("rtbprovince", StringType),
      StructField("rtbcity", StringType),
      StructField("rtbdistrict", StringType),
      StructField("rtbstreet", StringType),
      StructField("storeurl", StringType),
      StructField("realip", StringType),
      StructField("isqualityapp", IntegerType),
      StructField("bidfloor", DoubleType),
      StructField("aw", IntegerType),
      StructField("ah", IntegerType),
      StructField("imeimd5", StringType),
      StructField("macmd5", StringType),
      StructField("idfamd5", StringType),
      StructField("openudidmd5", StringType),
      StructField("androididmd5", StringType),
      StructField("imeisha1", StringType),
      StructField("macsha1", StringType),
      StructField("idfasha1", StringType),
      StructField("openudidsha1", StringType),
      StructField("androididsha1", StringType),
      StructField("uuidunknow", StringType),
      StructField("userid", StringType),
      StructField("iptype", IntegerType),
      StructField("initbidprice", DoubleType),
      StructField("adpayment", DoubleType),
      StructField("agentrate", DoubleType),
      StructField("lomarkrate", DoubleType),
      StructField("adxrate", DoubleType),
      StructField("title", StringType),
      StructField("keywords", StringType),
      StructField("tagid", StringType),
      StructField("callbackdate", StringType),
      StructField("channelid", StringType),
      StructField("mediatype", IntegerType)
    )
  )
}
