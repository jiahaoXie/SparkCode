import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 运用SparkSQL的内置函数 根据每天的用户访问日志和用户购买日志，统计每日的uv
  * UV指的是 对用户系你才能够去重之后的访问总数
  * @author xjh 2018.12.05
  */
object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("DailyUV").setMaster("local")
    val spark = SparkSession.builder().appName("DailyUV").config(conf).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //构造用户访问日志数据，并创建相应的DataFrame
    //模拟用户访问日志，日志用逗号隔开，第一列是日期，第二列是用户id
    val userAccessLog=Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123"
    )
    val userAccessLogRDD=spark.sparkContext.parallelize(userAccessLog,5)
    // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD，转换为元素为Row的RDD
    val userAccessLogRowRDD=userAccessLogRDD.map{
      log=>Row(log.split(",")(0),log.split(",")(1).toInt)
    }
    //构造DataFrame的数据
    val structType=StructType(Array(
      StructField("date",StringType,true),
      StructField("userid",IntegerType,true)
    ))
    //创建DataFrame
    val userAccessLogRowDF=spark.createDataFrame(userAccessLogRowRDD,structType)
    // UV指的是 对用户系你才能够去重之后的访问总数
    // 这里，正式开始使用Spark 1.5.x版本提供的最新特性，内置函数，countDistinct
    // 讲解一下聚合函数的用法
    // 首先，对DataFrame调用groupBy()方法，对某一列进行分组
    // 然后，调用agg()方法 ，第一个参数，必须，必须，传入之前在groupBy()方法中出现的字段
    // 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    userAccessLogRowDF.groupBy("date")
      .agg('date,countDistinct('userid))  //需要导入包import org.apache.spark.sql.functions._
//      .map{ row => Row(row(1), row(2)) }  //不知道为什么这行为什么会报错？？ 注释掉这句 输出的内容有三列 前两列相同
      .collect()
      .foreach(println)
  }
}
