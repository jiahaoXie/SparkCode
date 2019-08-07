import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Parquet模式合并 学生信息合并（Parquet3结合案例）  //？？？ 有点问题？？
  */
object Parquet4 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Parquet4").setMaster("local")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val ageData=Array(("leo",23),("Jack",25))
    val ageDF=spark.sparkContext.parallelize(ageData,2).toDF("name","age")
    ageDF.show()
//    ageDF.write.parquet("hdfs://m1:9000/spark/sparkSQL/students/test_table/age")
    val gradeData=Array(("Marry","A"),("Jack","A+"),("kobe","B"))
    val gradeDF=spark.sparkContext.parallelize(gradeData,2).toDF("name","grade")
    gradeDF.show()
//    gradeDF.write.parquet("hdfs://m1:9000/spark/sparkSQL/students/test_table/grade")
    //合并不同Parquet中的数据 采用mergeSchema的方式
    val mergeDF=spark.read.option("mergeSchema","true").parquet("hdfs://m1:9000/spark/sparkSQL/students/test_table")
      //?? 这里居然报错了？？？无法推断Parquet的模式。必须手动指定。
    mergeDF.printSchema() //输出模式
//    mergeDF.show()  //输出表 格
  }
}
