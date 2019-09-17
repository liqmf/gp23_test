package Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计省市指标
  */
object Parquet2json {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输出目录不正确")
      sys.exit()
    }
    // 获取目录参数
    val  Array(inputPath) = args

    val sparkSession= SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = sparkSession.read.parquet(inputPath)
    df.createTempView("log")
    val df2 = sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    val dd = df.rdd
    //val parquetSchema = sqlContext.parquetFile(inputPath)
     val jsonstr: RDD[String] = dd.map(x => {
       ((x.getString(23), x.getString(24)), 1)
     }).reduceByKey(_ + _).map(x =>
       "{\"ct\":" + x._2 + ",\"provincename\":\"" + x._1._1 + "\",\"cityname\":\"" + x._1._2 + "\"}"
     )

    df2.write.partitionBy("provincename","cityname").json("C:\\Users\\wang_xuejia\\Desktop\\综合笔记\\Spark用户画像分析\\aa")
    //存Mysql

    //通过config配置文件依赖进行加载相关的配置信息
    val load = ConfigFactory.load()
    //创建properties对象
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))

    //存储

    df2.write.mode(SaveMode.Append).jdbc(
      load.getString("jdbc.url"),load.getString("jdbc.tableName"),prop
    )


  }
}
