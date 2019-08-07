import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import scala.collection.mutable.ListBuffer
/**
  * SparkCore+SparkSQL实现每日top3热点搜索词
  * @author xjh 2018.12.09
  * 以下是视频中的内容
  * 需求：
      1、筛选出符合查询条件（城市、平台、版本）的数据
      2、统计出每天搜索uv排名前3的搜索词
      3、按照每天的top3搜索词的uv搜索总次数，倒序排序
      4、将数据保存到hive表中
  实现思路分析：
      1、针对原始数据（HDFS文件），获取输入的RDD
      2、使用filter算子，去针对输入RDD中的数据，进行数据过滤，过滤出符合查询条件的数据。
        2.1 普通的做法：直接在fitler算子函数中，使用外部的查询条件（Map），但是，这样做的话，是不是查询条件Map，会发送到每一个task上一份副本。（性能并不好）
        2.2 优化后的做法：将查询条件，封装为Broadcast广播变量，在filter算子中使用Broadcast广播变量进行数据筛选。
      3、将数据转换为“(日期_搜索词, 用户)”格式，然后对它进行分组，然后再次进行映射，对每天每个搜索词的搜索用户进行去重操作，并统计去重后的数量，即为每天每个搜索词的uv。最后，获得“(日期_搜索词, uv)”
      4、将得到的每天每个搜索词的uv，RDD，映射为元素类型为Row的RDD，将该RDD转换为DataFrame
      5、将DataFrame注册为临时表，使用Spark SQL的开窗函数，来统计每天的uv数量排名前3的搜索词，以及它的搜索uv，最后获取，是一个DataFrame
      6、将DataFrame转换为RDD，继续操作，按照每天日期来进行分组，并进行映射，计算出每天的top3搜索词的搜索uv的总数，然后将uv总数作为key，将每天的top3搜索词以及搜索次数，拼接为一个字符串
      7、按照每天的top3搜索总uv，进行排序，倒序排序
      8、将排好序的数据，再次映射回来，变成“日期_搜索词_uv”的格式
      9、再次映射为DataFrame，并将数据保存到Hive中即可
  */
object DailyTop3KeyWord extends App {
  /** 设置日志等级 */
  Logger.getLogger("org").setLevel(Level.WARN)

  val conf=new SparkConf().setAppName("DailyTop3KeyWord").setMaster("local[4]")
  val spark=SparkSession.builder().config(conf).getOrCreate()
  //导入隐式转换
  import spark.implicits._
  //伪造一份数据 建立查询条件 实际应用中可以通过MySQL关系数据库查询
  //以Map(key/value)的形式保存
  var queryPara=Map(
    "city"->List("beijing","shanghai"),
    "platform"->List("android"),
    "version"->List("1.0","1.1","1.2","1.3")
  )

  //将查询条件封装为一个广播变量
  val queryParaBroadCast=spark.sparkContext.broadcast(queryPara)
  //读取数据 初始化RDD
  val searchLogRDD=spark.sparkContext.textFile("D:\\ideaCode\\spark_wordCount\\searchLog.txt")
//    .map(_.split(","))

  //广播变量 使用filter算子进行过滤 得到符合条件的数据
  val filterSearchLogRDD=searchLogRDD.filter(log=>{
    val city=log.split(",")(3)
    val platform=log.split(",")(4)
    val version=log.split(",")(5)
//    val city=log(3)
//    val platform=log(4)
//    val version=log(5)
    val queryParamValue=queryParaBroadCast.value
      //得到相应key下的value
    val citys=queryParamValue.get("city").get
    val platforms=queryParamValue.get("platform").get
    val versions=queryParamValue.get("version").get
    var flag=true
    if(!citys.contains(city)){
      flag=false
    }
    if(!platforms.contains(platform)){
      flag=false
    }
    if(!versions.contains(version)){
      flag=false
    }
    flag
  })
  //将过滤出来的日志映射成为 “日期_搜索词，用户”
  val dateKeywordRDD=filterSearchLogRDD.map(row=>(
    row.split(",")(0)+"_"+row.split(",")(2),row.split(",")(1)
//    row(0)+"_"+row(2),row(1)
  ))
  //进行分组 ，获取每天每个搜索词 有哪些用户搜索了（这里没有去重）
  val dateKeywordUserRDD=dateKeywordRDD.groupByKey()
  dateKeywordUserRDD.collect().foreach(println)
  //将相同行数合并，并计算用户访问的次数，"日期_搜索词，UV"(去重操作)
  val dateKeywordUVRDD=dateKeywordUserRDD.map(row=>{
    val dateKeyWord=row._1
    val users=row._2.iterator
    //对用户进行去重，并统计去重后的数量
    val distinctUsers=new ListBuffer[String]
    while (users.hasNext){
      val user=users.next().toString()
      if(!distinctUsers.contains(user)){
        distinctUsers.append(user)
      }
    }
    val uv=distinctUsers.size
    (dateKeyWord,uv)
  })
  //将"日期_搜索词"，uv转换为DataFrame
  val dateKeywordUVRowRDD=dateKeywordUVRDD.map(row=>
    Row(row._1.split("_")(0),row._1.split("_")(1),row._2.toString.toLong)
  )
  val structType=StructType(Array(
    StructField("date",StringType,true),
    StructField("keyword",StringType,true),
    StructField("uv",LongType,true)
  ))
  val dateKeywordUVDF=spark.createDataFrame(dateKeywordUVRowRDD,structType)
  //注册临时函数
  dateKeywordUVDF.createOrReplaceTempView("daily_keyword_uv")
    //利用spark sql开窗函数，统计每天搜索uv排名前三的搜索词
  val dailyTop3KeyWordDF=spark.sql(
    "select date,keyword,uv from (" +
      "select date,keyword,date,keyword,uv,row_number() over (partition by date order by uv desc)" +
      " rn from daily_keyword_uv) where rn<=3"
  )
  dailyTop3KeyWordDF.show()
  spark.sparkContext.stop()
}
