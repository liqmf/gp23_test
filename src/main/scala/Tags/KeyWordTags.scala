package Tags

import Utils.TagInterface
import org.apache.spark.sql.Row

object KeyWordTags extends  TagInterface{
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val keywords = row.getAs[String]("keywords")
    var list = List[(String,Int)]()
    if(!keywords.contains("\\|")) {
      if (keywords.length > 3 && keywords.length < 8) {
        list :+= ("K" + keywords, 1)
      }
    }else {
      val keyWordArr = keywords.split("\\|")
      for (i <- 0 until keyWordArr.length) {
        if (keyWordArr(i).length > 3 && keyWordArr(i).length < 8) {
          list :+= ("k" + keyWordArr(i), 1)
        }
      }
    }
    list
  }
}
