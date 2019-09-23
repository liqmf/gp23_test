package Tags

import Utils.TagInterface
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

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
object TagsLocation  extends TagInterface{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    // 获取地域数据
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(pro)){
      list :+=("ZP"+pro,1)
    }
    if(StringUtils.isNotBlank(city)){
      list :+=("ZC"+city,1)
    }
    list
  }

}
