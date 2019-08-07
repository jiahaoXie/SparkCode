import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
/**
  * 编程方式定义RDD模式
    *如果无法提前定义案例类（例如，记录的结构以字符串形式编码，或者文本数据集将被解析，而字段将针对不同的用户进行不同的投影），
    DataFrame则可以通过三个步骤以编程方式创建a 。
      * Row从原始RDD 创建s的RDD;
        创建由StructType匹配Row步骤1中创建的RDD中的s 结构 表示的模式。
        Row通过createDataFrame提供的方法将模式应用于s 的RDD SparkSession。
  * @author xjh 2018.11.28
  */
object RDDToDataFrame2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("RDDToDataFrame2").setMaster("local")
    val spark=SparkSession.builder().appName("RDDToDataFrame2").config(conf).getOrCreate()
    import spark.implicits._
    //创建RDD
    val peopleRDD=spark.sparkContext.textFile("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\" +
      "spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt")
    //定义一个模式字符串
    val schemaString="name age"
    //根据模式字符串生成模式schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema=StructType(fields)
    //将RDD(人员)的记录转换为行
    val rowRDD=peopleRDD.map(_.split(",")).map(attributes=>Row(attributes(0), attributes(1).trim))
    //将模式应用于RDD
    val peopleDF=spark.createDataFrame(rowRDD,schema)
    //使用DataFrame创建临时视图
    peopleDF.createOrReplaceTempView("people")
    //SQL可以在使用DataFrames创建的临时视图上运行
    val results = spark.sql("SELECT name,age FROM people")
//    results.map(attributes => "name: " + attributes(0)+","+"age:"+attributes(1))
    results.map(attributes => "name: " + attributes(0)+","+"age:"+attributes(1)).show()
        //官网上这句运行报错，需添加rdd 但是这样又不能写show() ????
  }
}
