import org.apache.spark.{SparkConf, SparkContext}

/**textFile创建RDD
  * 这里小案例是 是实现文件字数的统计
  * @author xjh 2018.11.19
  */
object TextFile {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TextFile").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd=sc.textFile("D:\\ideaCode\\spark_wordCount\\spark.txt")
      //textFile返回的RDD中，每个元素就是文件中的一行文本
    val wordCount=rdd.map(line=>line.length).reduce(_+_)
    println("wordCunt: "+wordCount)
  }
}
