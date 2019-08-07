import org.apache.spark.{SparkConf, SparkContext}

/**
  * 并行化集合创建RDD  调用SparkContext的parallelize()方法
  * 这里编程写一个1 to 10累加的操作
  * @author xjh 2018.11.19
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc=new SparkContext(conf)
    val number=Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD=sc.parallelize(number,5)  //5表示分区(partition)
    val sum=numberRDD.reduce(_+_)
    println("sum: "+sum)
  }
}
