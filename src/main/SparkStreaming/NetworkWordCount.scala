import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 单词计数wordcount
  * SparkStreaming Socket套接字流（DStream）
  * @author xjh 2018.09.28
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
//    if(args.length<2){
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    StreamingExamples.setStreamingLogLevels()
    // Create the context with a 10 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream("msiPC",9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
