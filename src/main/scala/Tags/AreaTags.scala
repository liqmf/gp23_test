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
object AreaTags extends  TagInterface{
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    var list = List[(String,Int)]()
    if(StringUtils.isNoneBlank(provincename)) list :+= ("ZP" + provincename, 1)
    if(StringUtils.isNoneBlank(cityname)) list :+= ("ZC" + cityname, 1)
    list
  }
}
