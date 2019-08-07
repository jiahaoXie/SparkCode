import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types._

/**
  * SparkStreaming与Spark SQL整合使用，top3热门商品实时统计
  * @author xjh 2018.12.17
  */
object Top3HotProduct {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    //创建2秒批量大小的StreamingContext
    val sparkConf = new SparkConf().setAppName("Top3HotProduct").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val productClickLogDStream=ssc.socketTextStream("msiPC",9999)
    //这里测试的输入日志格式为： xjh iphone phone
    val categoryProductPairDStream=productClickLogDStream.map(productClickLog=>(
      productClickLog.split(" ")(2)+"_"+productClickLog.split(" ")(1),1
    ))
    // 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗口操作
    // 第二个参数，是窗口长度，这里是60秒
    // 第三个参数，是滑动间隔，这里是10秒
    // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续计算
    val categoryProductCountDStream=categoryProductPairDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int)=>v1+v2,   //这里是对key进行累加操作
      Seconds(60),
      Seconds(10)
    )
    //进行foreachRDD输出操作
    categoryProductCountDStream.foreachRDD(categoryProductCountsRDD => {
      val categoryProductCountRowRDD = categoryProductCountsRDD.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)  //转换为Row[RDD]
      })

      val structType = StructType(Array(
        StructField("category",StringType,true),
        StructField("product", StringType, true),
        StructField("click_count", IntegerType, true))
      )

      val spark =new HiveContext(categoryProductCountsRDD.context)
      //这个HiveContext是spark1.6版本中的用法 在Spark2.x的版本中 用SparkSession来替代。
      // 这里没有改过来的原因是 暂时没能找到SparkSession类中实现对应功能的方法名

      val categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDD, structType)
      //将60秒内的每个种类的每个商品的点击次数的数据，注册为临时视图
      categoryProductCountDF.createOrReplaceTempView("product_click_log")
      //这里需要用到SparkSQL的开窗函数 ，注意开窗函数和SparkStreaming的窗口函数是不同的概念
      val top3ProductDF = spark.sql(
        "SELECT category,product,click_count "
          + "FROM ( SELECT category,product,click_count,row_number() " +
          "OVER (PARTITION BY category ORDER BY click_count DESC) rank "
          + "FROM product_click_log"
          + ") tmp "
          + "WHERE rank<=3")
      // 这里说明一下，其实在企业场景中，可以不是打印的
      // 案例说，应该将数据保存到redis缓存、或者是mysql db中
      // 然后，应该配合一个J2EE系统，进行数据的展示和查询、图形报表

      top3ProductDF.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
