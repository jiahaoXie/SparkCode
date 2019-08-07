import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD持久化
  * @author xjh 2018.11.20
  */
object Persist {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Persist").setMaster("local")
    val sc=new SparkContext(conf)
    val list=List("Hadoop","Hive","HBase","Spark")
    val l=sc.parallelize(list)
    l.cache() //cache()方法调用persist(MEMORY_ONLY) 语句执行到这里 并不会缓存该rdd 因为它是transformation操作
    println(l.count())  //执行action操作 触发一次从头到尾的计算，这时会执行上面的rdd持久化操作
    println(l.collect().mkString(","))  //执行第二次action操作时，就不需要从头到尾进行计算操作，只需要重复使用上面放入缓存中的rdd即可
    l.unpersist() //手动将持久化的rdd从缓存中移除

  }
}
