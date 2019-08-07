import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于滑动窗口的热点搜索词实时统计
  * @author xjh 2018.12.16
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf=new SparkConf().setAppName("WindowHotWord").setMaster("local[4]")
    val ssc=new StreamingContext(conf,Seconds(1))    //创建StreamingContext对象
    // 这里的数据源采取网络socket输入 虚拟机上输入 nc -lk 9999即可实现数据输入
    // 这里的搜索日志的格式：
      // leo hello
      // tom world
    val searchLogsDStream=ssc.socketTextStream("msiPC",9999)
    // 将搜索日志给转换成，只有一个搜索词，即可
    val searchWordsDStream=searchLogsDStream.map(_.split(" ")(1)) //取第二列元素
    // 将搜索词映射为(searchWord, 1)的tuple格式
    val searchWordPairDStream=searchWordsDStream.map(searchWord=>(searchWord,1))
    // 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗口操作
    // 第二个参数，是窗口长度，这里是60秒
    // 第三个参数，是滑动间隔，这里是10秒
    // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续计算
    // 所以说，这里的意思就是，之前的操作到searchWordPairDStream为止，其实，都是不会立即进行计算的
    // 而是只是放在那里
    // 然后，等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔上面设置的是1秒，所以之前
    // 60秒，就有60个RDD，给聚合起来，然后，统一执行redcueByKey操作
    // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD
    val searchWordCountsDStream=searchWordPairDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int)=>v1+v2,
      Seconds(60),
      Seconds(10)
    )
    // 到这里为止，就已经可以做到，每隔10秒钟，出来，之前60秒的收集到的单词的统计次数
    // 执行transform操作，因为一个窗口，就是一个60秒钟的数据，会变成一个RDD，然后，对这一个RDD
    // 根据每个搜索词出现的频率进行排序，然后获取排名前3的热点搜索词
    val finalDStream=searchWordCountsDStream.transform(searchWordCountRDD=>{
      val countSearchWordRDD=searchWordCountRDD.map(tuple=>(tuple._2,tuple._1)) // 执行搜索词和出现频率的反转
      val sortedCountSearchWordRDD=countSearchWordRDD.sortByKey(false)  //降序排列
      val sortedSearchWordCountRDD=sortedCountSearchWordRDD.map(tuple=>(tuple._2,tuple._1)) // 然后再次执行反转，变成(searchWord, count)的这种格式
      val top3SearchWordCountRDD=sortedSearchWordCountRDD.take(3)   //take(3)取排名前3的热点搜索词
      for (tuple<-top3SearchWordCountRDD){
        println(tuple)  //对前60s所有数据排名前三的数据输出
      }
      searchWordCountRDD
    })
    finalDStream.print()// 这是为了触发job的执行，所以必须有output操作

    ssc.start()
    ssc.awaitTermination()
  }

}
