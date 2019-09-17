package Location

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object area_result_RDD {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输出目录不正确")
      sys.exit()
    }
    val  Array(inputPath) = args
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = sparkSession.read.parquet(inputPath)
    val rdd = df.rdd
    val info_rdd: RDD[Basic_info] = rdd.map(x => {
      val provincename = x.getString(24)
      val cityname = x.getString(25)
      val requestmode = x.getInt(8)
      val processnode = x.getInt(35)
      val iseffective = x.getInt(30)
      val isbilling = x.getInt(31)
      val isbid = x.getInt(39)
      val iswin = x.getInt(42)
      val adorderid = x.getInt(2)
      val winprice = x.getDouble(41)
      val adpayment = x.getDouble(75)
      val ispid = x.getInt(26)
      val ispname = x.getString(27)
      val client = x.getInt(17)
      val networkmannerid = x.getInt(28)
      val networkmannername = x.getString(29)
      val devicetype = x.getInt(34)
      Basic_info(provincename, cityname, requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment, ispid, ispname, client, networkmannerid, networkmannername, devicetype)
    })
    val list = List(1,1,1,1,1,1,1)
    val test: RDD[(String, (Basic_info, List[Int]))] = info_rdd.map(x => {
      (x.provincename, (x, List(1, 1, 1, 1, 1, 1, 1)))
    }).reduceByKey((x, y) => {
      var list0 = x._2(0)
      var list1 = x._2(1)
      var list2 = x._2(2)
      var list3 = x._2(3)
      var list4 = x._2(4)
      var list5 = x._2(5)
      var list6 = x._2(6)
      if (x._1.requestmode == 1 && x._1.processnode >= 1 && y._1.requestmode == 1 && y._1.processnode >= 1) {
       list0 =  x._2(0) + y._2(0)
      }
      if (x._1.requestmode == 1 && x._1.processnode >= 2 && y._1.requestmode == 1 && y._1.processnode >= 2) {
        list1 =  x._2(1) + y._2(1)
      }
      if (x._1.requestmode == 1 && x._1.processnode ==3 && y._1.requestmode == 1 && y._1.processnode == 3) {
        list2 =  x._2(2) + y._2(2)
      }
      if(x._1.iseffective.equals("1")&&x._1.isbilling.equals("1")&& x._1.isbid.equals("1") && y._1.iseffective.equals("1") && y._1.isbilling.equals("1") && y._1.isbid.equals("1")){
        list3 = x._2(3)+y._2(3)
      }
      if(x._1.iseffective.equals("1")&&x._1.isbilling.equals("1")&& x._1.iswin.equals("1")&& x._1.adorderid != 0 && y._1.iseffective.equals("1") && y._1.isbilling.equals("1") && y._1.iswin.equals("1") && y._1.adorderid !=0){
        list4 = x._2(4)+y._2(4)
      }
      if (x._1.requestmode == 2 && x._1.iseffective.equals("1") && y._1.requestmode == 2 && y._1.iseffective.equals("1")) {
        list5 =  x._2(5) + y._2(5)
      }
      if (x._1.requestmode == 3 && x._1.iseffective.equals("1") && y._1.requestmode == 3 && y._1.iseffective.equals("1")) {
        list6 =  x._2(6) + y._2(6)
      }
      (x._1,List(list0,list1,list2,list3,list4,list5,list6))
    }).union(info_rdd.map(x => {
      ((x.provincename+" "+x.cityname), (x, List(1, 1, 1, 1, 1, 1, 1)))
    }).reduceByKey((x,y) => {
      var list0 = x._2(0)
      var list1 = x._2(1)
      var list2 = x._2(2)
      var list3 = x._2(3)
      var list4 = x._2(4)
      var list5 = x._2(5)
      var list6 = x._2(6)
      if (x._1.requestmode == 1 && x._1.processnode >= 1 && y._1.requestmode == 1 && y._1.processnode >= 1) {
        list0 =  x._2(0) + y._2(0)
      }
      if (x._1.requestmode == 1 && x._1.processnode >= 2 && y._1.requestmode == 1 && y._1.processnode >= 2) {
        list1 =  x._2(1) + y._2(1)
      }
      if (x._1.requestmode == 1 && x._1.processnode ==3 && y._1.requestmode == 1 && y._1.processnode == 3) {
        list2 =  x._2(2) + y._2(2)
      }
      if(x._1.iseffective.equals("1")&&x._1.isbilling.equals("1")&& x._1.isbid.equals("1") && y._1.iseffective.equals("1") && y._1.isbilling.equals("1") && y._1.isbid.equals("1")){
        list3 = x._2(3)+y._2(3)
      }
      if(x._1.iseffective.equals("1")&&x._1.isbilling.equals("1")&& x._1.iswin.equals("1")&& x._1.adorderid != 0 && y._1.iseffective.equals("1") && y._1.isbilling.equals("1") && y._1.iswin.equals("1") && y._1.adorderid !=0){
        list4 = x._2(4)+y._2(4)
      }
      if (x._1.requestmode == 2 && x._1.iseffective.equals("1") && y._1.requestmode == 2 && y._1.iseffective.equals("1")) {
        list5 =  x._2(5) + y._2(5)
      }
      if (x._1.requestmode == 3 && x._1.iseffective.equals("1") && y._1.requestmode == 3 && y._1.iseffective.equals("1")) {
        list6 =  x._2(6) + y._2(6)
      }
      (x._1,List(list0,list1,list2,list3,list4,list5,list6))
    }))

    test.map(x=>{
      (x._1,x._2._2)
    }).sortByKey().foreach(println)
  }
}
