import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
/**
  * UDAF：User Defined Aggregate Function。用户自定义聚合函数。是Spark 1.5.x引入的最新特性。
  * UDF，其实更多的是针对单行输入，返回一个输出
  * 这里的UDAF，则可以针对多行输入，进行聚合计算，返回一个输出，功能更加强大
  * @author xjh 2018.12.09
  */
object UDAF {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UDAF").setMaster("local")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    //构造模拟数据
    val names=Array("leo","xjh","kobe","Curry","James","xjh","kobe","KD")
    val nameRDD=spark.sparkContext.parallelize(names,4) //构造初始RDD
    val namesRowRDD=nameRDD.map(name=>Row(name))    //构造行RDD
    val structType=StructType(Array(StructField("name",StringType,true)))
    val nameDF=spark.createDataFrame(namesRowRDD,structType)  //构造DataFrame

    //注册一张name表
    nameDF.createOrReplaceTempView("names")

    //定义和注册自定义函数
    //定义函数 自己编写匿名函数
    //注册函数：spark.udf.register()
    spark.udf.register("strCount",new StringCount)   //这里写的是求字符串长度

    //使用自定义函数
    spark.sql("select name,strCount(name) from names group by name")
      .show()
    //      .collect().foreach(println)
  }
}
