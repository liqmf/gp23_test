package Tags


import org.apache.spark.sql.SparkSession

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输出目录不正确")
      sys.exit()
    }
    val  Array(inputPath,inputPath2) = args
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val broad_data = sparkSession.sparkContext.textFile(inputPath2)
                      .map(x=>{
                        x.split("\\s",-1)
                      })
                      .filter(_.length >=5)
                      .map(arr=>{
                        (arr(1),arr(4))
                      }).collectAsMap()
    val broadcast = sparkSession.sparkContext.broadcast(broad_data)
    df.rdd.map(row=>{
      val adTagList =  AdTags.makeTags(row)
      val appTagList = AppTags.makeTags(row,broadcast)
      val AdplatTagList = AdplatTags.makeTags(row)
      val EquipTagList = EquipmentTags.makeTags(row)
      val keyWordTagList = KeyWordTags.makeTags(row)
      val areaList = AreaTags.makeTags(row)
      val allList = adTagList ++ appTagList ++ AdplatTagList ++ EquipTagList ++ keyWordTagList ++ areaList
      val userid = row.getAs[String]("userid")
      (userid,allList)
    }).map(x=>{
      (x._1,x._2.mkString(","))
    }).foreach(println)
  }
}
