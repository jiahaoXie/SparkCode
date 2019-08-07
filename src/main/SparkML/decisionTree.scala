import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

case class Iris(feature: org.apache.spark.ml.linalg.Vector,label: String)

/**
  * decision tree
  * @author xjh 2018.10.26
  */
object decisionTree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

//    val data=spark.sparkContext.textFile("D:\\ideaCode\\iris.txt").map(_.split(","))
//      .map(p => Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble),p(4).toString())).toDF()
//
//    data.createOrReplaceTempView("iris")

  }
}
