package Location

import Utils.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

class App{

}
object App {
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("输出目录不正确")
      sys.exit()
    }
    val  Array(inputPath,outputpath,docs) = args
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //读取数据字典
    val docMap = sparkSession.sparkContext.textFile(docs).map(_.split("\\s", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    //进行广播
    val boradcast = sparkSession.sparkContext.broadcast(docMap)
    //读取数据文件
    val df = sparkSession.read.parquet(inputPath)
    df.rdd.map(row=>{

      //取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = boradcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val rptlist = RptUtils.Reqpt(requestmode,processnode)
      val clicklist = RptUtils.clickPt(requestmode,iseffective)
      val adlist = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val allList:List[Double] = rptlist ++ clicklist ++ adlist
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputpath)
  }
}
