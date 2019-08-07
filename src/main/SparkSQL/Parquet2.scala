import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Parquet数据源之自动推断分区  ??? 1.6以后的版本默认不再通过路径分区 运行结果不对
  * @author xjh 2018.12.01
  */
object Parquet2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Parquet2").setMaster("local")
    val spark=SparkSession.builder().appName("Parquet2").config(conf).getOrCreate()
    val userDF=spark.read.parquet("hdfs://m1:9000/spark/sparkSQL/users/gender=male/country=us/users.parquet")

    userDF.printSchema()
    /**
      * root
       |-- name: string (nullable = true)
       |-- favorite_color: string (nullable = true)
       |-- favorite_numbers: array (nullable = true)
       |    |-- element: integer (containsNull = true)
      */
    userDF.show()
    /**
      *+------+--------------+----------------+
        |  name|favorite_color|favorite_numbers|
        +------+--------------+----------------+
        |Alyssa|          null|  [3, 9, 15, 20]|
        |   Ben|           red|              []|
        +------+--------------+----------------+
      */
  }
}
