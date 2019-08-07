import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xjh 2018.12.08
  */
object GroupTopN4 {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf=new SparkConf().setAppName("GroupTopN2").setMaster("local[4]")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\score4.txt")
    val classScores=lines.map(line=>(
      line.split(" ")(0),line.split(" ")(1),line.split(" ")(2).toInt,line.split(" ")(3).toInt,line.split(" ")(4).toInt
      ,line.split(" ")(5).toInt
    ))
    //根据班级姓名排序
    val topK = classScores.groupBy(item => (item._1)).map(subG=>{
      val (classId) = subG._1
      val DataStrutureTopK=subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(5).map(item=>item._3+"分:姓名"+item._2)
      val NetworkTopK=subG._2.toList.sortBy(_._4)(Ordering.Int.reverse).take(5).map(item=>item._4+"分:姓名"+item._2)
      val OperationTopK=subG._2.toList.sortBy(_._5)(Ordering.Int.reverse).take(5).map(item=>item._5+"分:姓名"+item._2)
      val DatabaseTopK=subG._2.toList.sortBy(_._6)(Ordering.Int.reverse).take(5).map(item=>item._6+"分:姓名"+item._2)
      (classId,Map("数据结构前5"->DataStrutureTopK,"计算机网络前5" -> NetworkTopK, "操作系统前5" -> OperationTopK
        ,"数据库前5"->DatabaseTopK
      ))
    })
    topK.foreach(println)
  }
}
