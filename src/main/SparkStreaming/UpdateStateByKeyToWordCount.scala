import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用UpdateStateByKey操作进行单词计数（基于缓存的实时wordcount程序）
  * SparkStreaming和SparkSQL结合使用
  * @author xjh 2018.12.14
  */
object UpdateStateByKeyToWordCount {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf=new SparkConf().setAppName("UpdateStateByKeyToWordCount").setMaster("local[4]")
    val ssc=new StreamingContext(conf,Seconds(10))    //创建StreamingContext对象
    // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
    // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
    // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
    // 内存数据丢失的时候，可以从checkpoint中恢复数据
    //开启checkpoint机制
    ssc.checkpoint("hdfs://msiPC:9000/spark/mycode/checkpointLoc")
    val lines =ssc.textFileStream("hdfs://msiPC:9000/spark/mycode/streaming")
    val words=lines.flatMap(_.split(" "))
    val pairs=words.map((_,1))
    /**
      * updateStateByKey可以实现直接通过Spark维护一份每个单词的全局的统计次数
      * pairs.updateStateByKeyInt每次传递给updateFunc函数两个参数:
        * 第一个参数values是某个key（即某个单词）的当前批次的一系列值的列表（Seq[Int]形式）,计算这个被传递进来的与某个key对应的当前批次的所有值的总和，也就是当前批次某个单词的出现次数。
      * 传递给updateFunc函数的第二个参数是某个key的历史状态信息，也就是某个单词历史批次的词频汇总结果。实际上，某个单词的历史词频应该是一个Int类型，
      * 这里为什么要采用Option[Int]呢？
          Option[Int]是类型 Int的容器,你可以把它看作是某种集合，这个特殊的集合要么只包含一个元素（即单词的历史词频），
          要么就什么元素都没有（这个单词历史上没有出现过，所以没有历史词频信息）。
          之所以采用 Option[Int]保存历史词频信息，这是因为，历史词频可能不存在。在值不存在时，需要进行回退，或者提供一个默认值，
          Scala 为Option类型提供了getOrElse方法，以应对这种情况。
          state.getOrElse(0)的含义是，如果该单词没有历史词频统计汇总结果，那么，就取值为0，如果有历史词频统计结果，就取历史结果。
      */
    val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for(value <- values) {
        newValue += value
      }
      Option(newValue)
    })
    wordCounts.print()
    ssc.start() //开始计算
    ssc.awaitTermination()  //等待计算结束
  }
}
