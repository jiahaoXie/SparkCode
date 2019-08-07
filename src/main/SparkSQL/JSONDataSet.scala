import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * JSON 数据集
  * @author xjh 2018.12.01
  */
object JSONDataSet {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("JSONDataSet").setMaster("local")
    val spark=SparkSession.builder().appName("JSONDataSet").config(conf).getOrCreate()
    //在创建数据集时通过导入spark.implicits._ 来支持原始类型（Int，String等）和产品类型（案例类）编码器。
    import spark.implicits._
    //路径指向JSON数据集。
    //路径可以是单个文本文件，也可以是存储文本文件的目录
    val peopleDF=spark.read.json("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\" +
      "spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
    //可以使用printSchema()方法可视化推断出的模式
    peopleDF.printSchema()    //运行结果如下图所示
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    //创建临时视图来使用DataFrame
    peopleDF.createOrReplaceTempView("people")
    //可以使用spark提供的SQL方法运行SQL语句
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    /**
      * +------+
        |  name|
        +------+
        |Justin|
        +------+
      */
    //或者，可以为JSON数据集创建一个DataFrame，该数据集由每个字符串存储一个JSON对象的数据集[String]表示
//    val otherPeopleDS=spark.createDataset(
//      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil
//    )
//    val otherPeople = spark.read.json(otherPeopleDS)
//    otherPeople.show()
  }
}
