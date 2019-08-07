//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.flume._
//import org.apache.spark.util.IntParam
///**
//  * SparkStreaming集成Flume
//  * @author xjh 2018.10.11
//  */
//object StreamingToFlume {
//  def main(args: Array[String]): Unit = {
//    StreamingExamples.setStreamingLogLevels()
//    val Array(host, IntParam(port)) = ["msiPC","44444"]
//    val batchInterval = Milliseconds(2000)
//    // Create the context and set the batch size
//    val sparkConf = new SparkConf().setAppName("FlumeEventCount").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, batchInterval)
//    // Create a flume stream
//    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)
//    // Print out the count of events received from this server in each batch
//    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//  }
//}
