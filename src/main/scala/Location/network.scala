package Location

import Utils.RptUtils
import org.apache.spark.sql.SparkSession

object operator {
  def main(args: Array[String]): Unit = {
   if(args.length != 1){
     println("输入目录不正确")
     sys.exit()
   }
    val Array(inputPath) = args

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df = sparkSession.read.parquet(inputPath)

    df.rdd.map(row=>{
      val networkmannerid= row.getAs[Int]("networkmannerid")
      val networkmannername = row.getAs[String]("networkmannername")
      var networkname ="2G"
      networkmannername.toUpperCase match {
        case "2G" =>  networkname = "2G"
        case "3G" =>  networkname = "3G"
        case "4G" =>  networkname = "4G"
        case "WIFI" =>  networkname ="WiFi"
        case _ =>  networkname ="其他"
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
      val requestList = RptUtils.Reqpt(requestmode,processnode)
      val adptList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val clickList = RptUtils.clickPt(requestmode,iseffective)

      val allList = requestList ++ adptList ++ clickList
      ((networkname,networkmannerid),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(x=>{
      var successpercent =0.0
      if(x._2(3) != 0)
        {
          successpercent = x._2(4)/x._2(3)
        }
      var clickpercent = 0.0
      if(x._2(7) != 0){
        clickpercent = x._2(8)/x._2(7)
      }
      (x._1,x._2(0).toInt,x._2(1).toInt,x._2(2).toInt,x._2(3).toInt,x._2(4).toInt,successpercent,x._2(7).toInt,x._2(8).toInt,clickpercent,x._2(5).toInt,x._2(6).toInt)
    }).sortBy(_._1._2).map(x=>{
      (x._1._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12)
    }).foreach(println)


  }
}
