import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Parquet模式合并(官网案例)
  * @author xjh 2018.12.01
  */
object Parquet3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Parquet3").setMaster("local")
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Parquet3").config(conf).getOrCreate()
    import spark.implicits._
    //用于隐式的将RDD转换为DataFrame

    //创建一个简单的DataFrame,存储到分区目录
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.show()
//    squaresDF.write.parquet("hdfs://m1:9000/spark/sparkSQL/Parquet3/test_table/key=1")
    //在新的分区目录中创建另一个DataFrame 添加新列并删除现有列
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
//    cubesDF.write.parquet("hdfs://m1:9000/spark/sparkSQL/Parquet3/test_table/key=2")
    cubesDF.show()
    //读取分区表
    val mergeDF=spark.read.option("mergeSchema","true").parquet("hdfs://m1:9000/spark/sparkSQL/Parquet3/test_table")
      //读取Parquet文件时，将数据源的选项，mergeSchema设置为true
    mergeDF.printSchema()
    mergeDF.show()
  }
}
