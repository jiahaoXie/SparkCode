import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * SparkSQL和SparkStreaming结合使用
  * 案例：使用DataFrame和SQL计算 单词计数[官网案例]
  * @author xjh 2018.12.16
  */
object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    //创建2秒批量大小的StreamingContext
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //在目标ip：port上创建套接字流并计算
    // \ n分隔文本的输入流中的单词（例如，由'nc -k '生成）
    //请注意，只有本地运行才能在存储级别重复。
    //在分布式方案中为容错进行必要的复制。
    val lines = ssc.socketTextStream("msiPC",9999)
    val words = lines.flatMap(_.split(" "))

    //将单词DStream的RDD转换为DataFrame并运行SQL查询
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      //获取SparkSession的单例实例
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")  //查询单词及其计数
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)


  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
