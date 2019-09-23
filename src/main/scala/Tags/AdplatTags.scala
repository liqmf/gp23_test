package Tags

import Utils.TagInterface
import org.apache.spark.sql.Row
import org.bouncycastle.pqc.math.linearalgebra.IntUtils

object AdplatTags extends TagInterface{
  override def makeTags(args: Any*): List[(String, Int)] = {
     val row = args(0).asInstanceOf[Row]
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    List[(String,Int)](("CN"+adplatformproviderid,1))
  }
}
