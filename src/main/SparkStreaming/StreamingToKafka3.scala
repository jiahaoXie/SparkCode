import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * KafkaUtils.createDirectStream方式（基于kafka低级API--偏移量由客户端保存）
  * 利用sparkStreaming对接kafka实现单词计数--采用Direct(低级API)
  * @author xjh 2018.10.10
  */
object StreamingToKafka3 {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf=new SparkConf().setAppName("SparkStreamingToKafka_Direct").setMaster("local[2]")
    //2.创建sparkContext
    val sc=new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3.创建StreamingContext
    val ssc=new StreamingContext(sc,Seconds(5))
    //4.配置kafka相关参数
    val kafkaParams=Map("metadata.broker.list"->"msiPC:9092,msiPC:9093,msiPC:9094","group.id"->"Kafka_Direct")
    //5.定义topic
    val topics = Set("my-replicated-topic")
    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //7.获取kafka中的topic中的数据
    val topicData:DStream[String]=dstream.map(_._2)
    //8.切分每一行，每个单词计为1
    val wordAndOne:DStream[(String,Int)]=topicData.flatMap(_.split(" ")).map((_,1))
    //9.相关单词出现的次数累加
    val result:DStream[(String,Int)]=wordAndOne.reduceByKey(_+_)
    //10.打印输出
    result.print()

    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }
}
