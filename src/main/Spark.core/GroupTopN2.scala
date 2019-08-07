import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组TopN
  * @author xjh 2018.12.08
  */
object GroupTopN2 {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf=new SparkConf().setAppName("GroupTopN2").setMaster("local[4]")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\score2.txt")
    val classScores=lines.map(line=>(
      line.split(" ")(0),line.split(" ")(1),line.split(" ")(2).toInt
    ))
    //根据班级姓名排序
    val topK = classScores.groupBy(item => (item._1)).map(subG=>{
      val (classId) = subG._1
      val scoreTopK=subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(5).map(item=>item._3+"分:姓名"+item._2)
      (classId,Map("分数前5："->scoreTopK))
    })
    topK.foreach(println)
  }
}
