import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming文件流  单词计数
  * @author xjh 2018.09.28
  */
object StreamingToFile {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("WordCuntStreaming").setMaster("local[2]")
    //设置为本地运行模式 2个线程 1个监听 宁一个处理数据
    val ssc=new StreamingContext(sparkConf,Seconds(15))  //时间间隔为秒
    val lines =ssc.textFileStream("hdfs://msiPC:9000/spark/mycode/streaming")
//    val lines =ssc.textFileStream("D:\\ideaCode\\spark_wordCount\\StreamingTestData")
    val words=lines.flatMap(_.split(" "))
    val wordCounts=words.map((_,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start() //开始计算
    ssc.awaitTermination()  //等待计算结束
  }
}
