import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
/**
  * KafkaUtils.createDirectStream方式sparkStreaming对接kafka实现单词计数—-采用Direct
        *  （低级API通过checkPoint恢复StreamingContext)
  * 利用sparkStreaming对接kafka实现单词计数--通过checkpint目录来构建StreamingContext
  * @author xjh 2018.10.10
  * 注意：在实际应用中，通常是推荐用Kafka Direct方式的，特别是现在随着Spark版本的提升，越来越完善这个Kafka Direct机制。
    * 优点：1、不用receiver，不会独占集群的一个cpu core；
          * 2、有backpressure自动调节接收速率的机制；
  */
object StreamingToKafka2 {

  //获取StreamingContext
  def createFunc(checkPointPath: String): StreamingContext = {
    //1、创建sparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkStreamingKafka_checkpoint")
      .setMaster("local[2]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint(checkPointPath)

    //4、配置kafka相关参数
    val kafkaParams = Map("metadata.broker.list" -> "msiPC:9092,msiPC:9093,msiPC:9094", "group.id" -> "Kafka_Direct")
    //5、定义topic
    val topics = Set("my-replicated-topic")
    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //7、获取kafka中topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)
    //8、切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
    //9、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    result.checkpoint(Seconds(10))
    //10、打印输出
    result.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    //定义checkpoint目录
    val checkPointPath = "./kafka_checkpoint"

    //StreamingContext.getOrCreate
    //如果设置了checkpoint目录并且里面数据没有问题，可以从checkpoint目录中构建一个StreamingContext，
    //如果没有设置checkpoint目录，他会重新构建
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPointPath, () => {
      createFunc(checkPointPath)
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
