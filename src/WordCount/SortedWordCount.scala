import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现基于排序机制的wordcount（降序输出）
  * @author xjh 2018.11.20
  */
object SortedWordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\spark.txt")
    val pairs=lines.flatMap(_.split(" ")).map((_,1))
    val wordCounts=pairs.reduceByKey(_+_)
    //以上的操作和原始的单词计数一样 后买你进行排序 并降序输出
    val countWords=wordCounts.map(wordCounts=>(wordCounts._2,wordCounts._1))  //首先对key/value进行反转
    val sortedCountWord=countWords.sortByKey(false) //降序排列 key值
    val sortedCount=sortedCountWord.map(word=>(word._2,word._1))  //再进行一次反转 负负得正
    sortedCount.foreach(println)
  }
}
