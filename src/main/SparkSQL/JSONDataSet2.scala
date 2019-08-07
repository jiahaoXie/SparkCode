import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * JSON数据运用 查询学生成绩中80分以上的（成功运行）
  * @author xjh 2018.12.01
  */
object JSONDataSet2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JSONDataSet2").setMaster("local")
    val spark = SparkSession.builder().appName("JSONDataSet2").config(conf).getOrCreate()
    //在创建数据集时通过导入spark.implicits._ 来支持原始类型（Int，String等）和产品类型（案例类）编码器。
    import spark.implicits._
    //路径指向JSON数据集。
    //路径可以是单个文本文件，也可以是存储文本文件的目录
    val studentScoreDF = spark.read.json("hdfs://m1:9000/spark/sparkSQL/students.json")
    //查询出分数大于80分的学生成绩信息，以及学生姓名
    studentScoreDF.createOrReplaceTempView("student_scores")
    val goodStudentScoresDF = spark.sql("select name,score from student_scores where score>=80")
    val goodStudentNames = goodStudentScoresDF.rdd.map { row => row(0) }.collect()  //得到学生姓名

    // 创建学生基本信息DataFrame
    val studentInfoJSONs = Array(
      "{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}"
    )
    val studentInfoJSONsRDD = spark.sparkContext.parallelize(studentInfoJSONs, 3)
    val studentInfosDF = spark.read.json(studentInfoJSONsRDD)

    //查询分数大于80分的学生基本信息
    studentInfosDF.createOrReplaceTempView("student_infos")
    var sql = "select name,age from student_infos where name in ("
    for (i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if (i < goodStudentNames.length - 1) {
        sql += ","
      }
    }
    sql += ")"
    val goodStudentInfosDF = spark.sql(sql)
    // 将分数大于80分的学生的成绩信息与基本信息进行join
    val goodStudentsRDD =
      goodStudentScoresDF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("score")) }
        .join(goodStudentInfosDF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("age")) })

    // 将rdd转换为dataframe
    val goodStudentRowsRDD = goodStudentsRDD.map(
      info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)))

    val goodStudentsDF = spark.createDataFrame(goodStudentRowsRDD, structType)

    // 将dataframe中的数据保存到json中
    goodStudentsDF.write.format("json").save("hdfs://m1:9000/spark/sparkSQL/good-students-scala")
  }
}
