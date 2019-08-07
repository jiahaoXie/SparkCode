import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现TopN
  * @author xjh 2018.11.21
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TopN").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\top.txt")
    val pairs=lines.map(line=>(line.toInt,line))  //这里时创建键值对RDD（pairRDD）
    val sortedPairs=pairs.sortByKey(false)    //降序排列
    val DescSort=sortedPairs.map(sortedPairs=>sortedPairs._1)
    DescSort.foreach(println) //这里降序输出所有值

    val topNum=sortedPairs.map(sortedPair=>sortedPair._1).take(3)
      //取pairRDD得第一列元素 并且时降序排列后得前三个
    for (num<-topNum)
      print(num+" ")
  }
}
