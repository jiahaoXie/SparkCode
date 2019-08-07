import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *利用反射机制将RDD转换为DataFrame
    * Spark SQL的Scala接口支持自动将包含RDD的案例类转换为DataFrame。
    * case类定义表的模式。使用反射读取case类的参数名称，并成为列的名称。
    * 案例类也可以嵌套或包含复杂类型，如Seqs或Arrays。可以将此RDD隐式转换为DataFrame，然后将其注册为表。
    * 表可以在后续SQL语句中使用。
  * @author xjh 2018.11.28
  */
case class Person(name: String, age: Long)
object RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CreateDataFrame").setMaster("local")
    val spark=SparkSession.builder().appName("CreateDataFrame").config(conf).getOrCreate()
    import spark.implicits._  //这里的spark不是某个包下面的东西，而是我们SparkSession.builder()对应的变量值
    val peopleDF=spark.sparkContext
      .textFile("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()
      //从文本文件创建Person对象的RDD,将其转换为DataFrame
    peopleDF.createOrReplaceTempView("people")  //必须注册为临时表才能供下面的查询使用
    val personsRDD=spark.sql("select name,age from people where age > 20")
    personsRDD.map(t => "Name:"+t(0)+","+"Age:"+t(1)).show()
      //DataFrame中的每个元素都是一行记录，包含name和age两个字段，分别用t(0)和t(1)来获取值
  }
}
