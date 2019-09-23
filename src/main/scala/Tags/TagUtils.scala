package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {

  // 取出唯一不为空的Id
  def getOneUserId(row:Row):String={
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM: "+v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MC: "+v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OD: "+v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AD: "+v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "ID: "+v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IMM: "+v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MCM: "+v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "ODM: "+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "ADM: "+v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDM: "+v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMS: "+v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MCS: "+v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "ODS: "+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "ADS: "+v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDS: "+v.getAs[String]("idfasha1")
      case _ =>"其他"
    }
  }
}
