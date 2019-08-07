import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * 单词计数
  * @author xjh 2018.09.24
  */
object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "D:\\BigData\\hadoop-2.6.0\\README.txt"
//    val inputFile="hdfs://msiPC:9000/input/README.txt"
    val conf = new SparkConf().setAppName("WordCount")
//      .setMaster("spark://m1:7077")  //表示standalone集群模式  尴尬：这里到集群上运行没有成功
//        .setJars(List("D:\\ideaCode\\spark_wordCount\\out\\artifacts\\WordCount_jar\\WordCount.jar"))
      .setMaster("local") //locali表示单机运行
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFile)  //lines RDD
//    val wordCount = textFile.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    val words=lines.flatMap(_.split(" "))
    val pairs=words.map((_,1))
    val wordCount=pairs.reduceByKey(_+_)
    wordCount.foreach(println)
//    wordCount.saveAsTextFile("hdfs://m1:9000/output/spark/result_wordCount") //上传到hdfs中指定的目录中
  }

  //flatMap源代码
//  def flatMap[U](f : scala.Function1[T, scala.TraversableOnce[U]])
//                (implicit evidence$4 : scala.reflect.ClassTag[U]) : org.apache.spark.rdd.RDD[U] = { /* compiled code */ }
}
