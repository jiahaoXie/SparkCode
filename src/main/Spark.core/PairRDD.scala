import org.apache.spark.{SparkConf, SparkContext}

/**
  * PairRDD运用：统计文件每行出现的次数
  * Spark有些特殊的算子，也就是特殊的transformation操作。比如groupByKey、sortByKey、reduceByKey等，其实只是针对特殊的RDD的。即包含key-value对的RDD。
  * 而这种RDD中的元素，实际上是scala中的一种类型，即Tuple2，也就是包含两个值的Tuple。
  * @author xjh 2018.11.19
  */
object PairRDD {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("PairRDD").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\hello.txt")
    val linePairs=lines.map((_,1))
    val lineCounts=linePairs.reduceByKey(_+_)
    lineCounts.foreach(println)
    //最后这句foreach为action操作，其余为transformation操作。
  }
}
