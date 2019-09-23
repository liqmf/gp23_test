package Tags

import Utils.TagInterface
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AreaTags extends  TagInterface{
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    var list = List[(String,Int)]()
    if(StringUtils.isNoneBlank(provincename)) {
      list :+= ("ZP" + provincename, 1)
    }
    if(StringUtils.isNoneBlank(cityname)) {
      list :+= ("ZC" + cityname, 1)
    }
    list
  }
}
