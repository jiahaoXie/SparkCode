import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用spark求出每个院系每个班每个专业前3名。
  * ？？？有问题 没运行出来？
  *
  * @author xjh 2018.12.08
  */
object GroupTopN3 {
  def main(args: Array[String]): Unit = {
    /** 设置日志等级 */
    Logger.getLogger("org").setLevel(Level.WARN)

    /** 设置spark环境 */
    val conf = new SparkConf().setAppName("GroupTopN3").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /** 原始数据解析 */
    val studentsScore = sc.textFile("D:\\ideaCode\\spark_wordCount\\score3.txt")
          .map(_.split(","))
        val groups=studentsScore.map(scoreInfo=>(
         scoreInfo(1),scoreInfo(2).toInt,scoreInfo(3).toInt,scoreInfo(4).toInt,scoreInfo(5),scoreInfo(6)
        ))
//    val groups = studentsScore.map(scoreInfo => (
//      scoreInfo.split(",")(1), scoreInfo.split(",")(2).toInt, scoreInfo.split(",")(3).toInt,
//      scoreInfo.split(",")(4).toInt, scoreInfo.split(",")(5), scoreInfo.split(",")(6)
//    ))

    /** 多次分组取TopK */
    val topK = groups.groupBy(item => (item._6, item._5)).map(subG => {
      val (departmentId, classId) = subG._1
      //语文前3
      val languageTopK=subG._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(3).map(item=>item._2+"分:学号"+item._1)
      //数学前3
      val mathTopK = subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(3).map(item => item._3 + "分:学号" + item._1)
      //外语前3
      val englishTopK = subG._2.toList.sortBy(_._4)(Ordering.Int.reverse).take(3).map(item => item._4 + "分:学号" + item._1)
      (departmentId, classId, Map("语文前3"->languageTopK,"数学前3" -> mathTopK, "外语前3" -> englishTopK))
    })

    /** 结果显示 */
    topK.foreach(println)
  }
}
