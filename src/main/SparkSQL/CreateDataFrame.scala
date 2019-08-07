import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 通过SparkSession创建DataFrame
  * @author xjh 2018.11.26
  */
object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CreateDataFrame").setMaster("local")
    val spark=SparkSession.builder().appName("CreateDataFrame").config(conf).getOrCreate()
    val df=spark.read.json("hdfs://m1:9000/spark/people.json")
    df.show()
  }
}
