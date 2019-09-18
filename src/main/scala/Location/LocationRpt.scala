package Location

import Utils.RptUtils
import org.apache.spark.sql.SparkSession

object LocationRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输出目录不正确")
      sys.exit()
    }
    val  Array(inputPath,outputpath) = args
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //获取数据
    val df = sparkSession.read.parquet(inputPath)

    df.rdd.map(row=>{
      //根据指标的字段获取数据
      val provincename = row.getString(24)
      val cityname = row.getString(25)
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //处理请求数
      val rptlist = RptUtils.Reqpt(requestmode,processnode)
      val clicklist = RptUtils.clickPt(requestmode,iseffective)
      val adlist = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val allList:List[Double] = rptlist ++ clicklist ++ adlist
      ((row.getAs[String]("provincename"),(row.getAs[String]("cityname"))),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputpath)

  }

}
