import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.{IntParam, LongAccumulator}
import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.rdd.RDD

/**
  * 累加器，广播变量 和检查点 案例实战（官网案例 main方法里面有地方报错？？？）
  * @author xjh 2018.12.17
  */

/**
  * 使用单例来获取或注册Broadcast变量
  */
object WordBlacklist {
  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

/**
  * 使用单例来获取或注册累加器
  */
object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}

/**
  * 计算每秒从网络接收的UTF8编码的文本中的单词。
  * 这个例子也是显示如何为累加器和广播使用延迟实例化的单例实例
  * 他们可以在driver失败时注册。
  *
  * @author xjh 2018.12.17
  */
object RecoverableNetworkWordCount {
  def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String): StreamingContext = {
    //如果你没看到下面的打印，说明从新的checkpoint中StreamingContext已经加载了
    println("Creating new context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[4]")
    //创建1秒batch大小的context
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    //创建一个socket stream输入源
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      // Get or register the blacklist Broadcast
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      // Get or register the droppedWordsCounter Accumulator
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      // Use blacklist to drop words and use droppedWordsCounter to count them
      val counts = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ", ", "]")
      val output = s"Counts at time $time $counts"
      println(output)
      println(s"Dropped ${droppedWordsCounter.value} word(s) totally")
      println(s"Appending to ${outputFile.getAbsolutePath}")
      Files.append(output + "\n", outputFile, Charset.defaultCharset())
    }
    ssc
  }
  def main(args: Array[String]): Unit = {
    val Array(ip, port, checkpointDirectory, outputPath)=Array("msiPC",9999,"hdfs://msiPC:9000/spark/mycode/checkpoint/RNWordCunt",
      "hdfs://msiPC:9000/spark/mycode/checkpoint/RecoverableNetworkWordCount")
    //  val ssc =StreamingContext.getOrCreate(checkpointDirectory,
    //    () => createContext(ip, port, outputPath, checkpointDirectory))
    //  //？？？？ 这里有问题？？ 运行报错
    //  ssc.start()
    //  ssc.awaitTermination()
    println("hello")
  }
}



