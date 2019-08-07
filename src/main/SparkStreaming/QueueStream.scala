import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable
/**
  * Streaming RDD队列流
  * @author xjh 2018.09.28
  */
object QueueStream {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("TestRDDQueue").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(10))
    val rddQueue=new mutable.SynchronizedQueue[RDD[Int]] ()
    val queueStream=ssc.queueStream(rddQueue)
    val mappedStream=queueStream.map(r=>(r%10,1))
    val reduceStream=mappedStream.reduceByKey(_+_)
    reduceStream.print()
    ssc.start()
    //Spark Streaming开始循环监听
    for(i<- 1 to 10){
      rddQueue+=ssc.sparkContext.makeRDD(1 to 100,2)
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}
