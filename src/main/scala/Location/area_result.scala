package Location

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object area_result {

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
    import  org.apache.spark.sql.functions._
//    val group_proandcity: DataFrame = df.groupBy("provincename","cityname").agg(count("iswin"))
//    df.groupBy("provincename").agg(count("iswin")).agg(expr = ).union(group_proandcity).orderBy("provincename").show()
//
    df.createTempView("area")
    import  sparkSession.implicits._
    val df2 = sparkSession.read.textFile("C://Users//wang_xuejia//Desktop//综合笔记//Spark用户画像分析//app_dict.txt")
    val media_dataset: Dataset[Media_caseclass] = df2.map(x => {
      val arr: Array[String] = x.split("\\s")
      if (arr.length > 4) {
        val appname = arr(1)
        val url = arr(4)
        Media_caseclass(appname, url)
      }
      else
        Media_caseclass("", "")
    }).filter(x => x.url != "")
    media_dataset.createTempView("media")

    val ds = sparkSession.sql("select provincename , cityname ,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from(select provincename , ''as cityname ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end)efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end)adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost  from area group by provincename union select provincename,cityname,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end)efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end)adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end)as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost  from area group by provincename,cityname)a order by provincename,cityname").show()
    sparkSession.sql("select case when ispname ='未知' then '其他' else ispname end ,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from (select ispid , ispname ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost from area group by ispid,ispname)a").show()
    sparkSession.sql("select networkmannername,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from (select networkmannerid , networkmannername ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost from area group by networkmannerid,networkmannername)a").show()
    sparkSession.sql("select case when devicetype = 1 then '手机' else case when devicetype=2 then '平板' else '其他' end end ,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from (select  devicetype ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost from area group by devicetype)a").show()
    sparkSession.sql("select case when client =1 then 'Android' else case when client =2 then 'ios' else case when client =3 then 'wp' else '其他' end end end system,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from (select client ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost from area group by client)a order by client").show()
    sparkSession.sql("select apptype ,basic_request,efficent_request,adv_request,cnt,sumsuccess,concat(cast(sumsuccess/cnt*100 as string),'%') successpercent,shownum,clicknum,concat(cast(clicknum/shownum *100 as string),'%') clickpercent,cousument/1000,cost/1000 from (select  case when substr(ar.appname,0,1) !='c' then ar.appname else m.appname end apptype ,sum(case when requestmode =1 and processnode >=1 then 1 else 0 end)basic_request,sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) efficent_request,sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adv_request,sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) as cnt,sum(case when iswin =1 then 1 else 0 end) sumsuccess,sum(case when requestmode =2 then 1 else 0 end) shownum,sum(case when requestmode =3 then 1 else 0 end) clicknum ,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  winprice else 0 end) cousument,sum(case when iseffective =1 and isbilling =1 and iswin =1 then  adpayment else 0 end) cost from area ar left join media m on ar.appname = m.url group by ar.appname,m.appname,apptype)a ").show()

  }
}

case class Media_caseclass(appname: String, url: String)

