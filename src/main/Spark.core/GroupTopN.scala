import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现分组TopN:二维数据 输出每个班级得前三名
  * @author xjh 2018.11.21
  */
object GroupTopN {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf=new SparkConf().setAppName("GroupTopN").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\score.txt")
    //拆分为Tuple2
    val classScores=lines.map(line=>(
      line.split(" ")(0),line.split(" ")(1).toInt
    ))

    //根据班级名 分组
    val group=classScores.groupByKey()
    //针对分组对数据及进行排序
    val groupSort=group.map(cs=>{
      val c=cs._1 //班级名字
      val s=cs._2 //分数值
      val scoreList=s.toList.sortWith(_>_).take(3)  //降序，并取前3个
      (c,scoreList)   //返回值
    })
    //遍历输出
    groupSort.foreach(v=>{
      print(v._1+" :")
      v._2.foreach(ss=>print(ss+"\t"))
      println()
    })


  }
}
