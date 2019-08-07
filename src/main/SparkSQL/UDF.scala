import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, StructType,StringType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * UDF(User Defined Function) 用户自定义函数
  * @author xjh 2018.12.09
  */
object UDF {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UDF").setMaster("local")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //1.构造模拟数据
    val names=Array("leo","xjh","kobe","James","KD")
    val nameRDD=spark.sparkContext.parallelize(names,4) //构造初始RDD
    val namesRowRDD=nameRDD.map(name=>Row(name))    //构造行RDD
    val structType=StructType(Array(StructField("name",StringType,true)))
    val nameDF=spark.createDataFrame(namesRowRDD,structType)  //构造DataFrame

    //注册一张name表
    nameDF.createOrReplaceTempView("names")

    //定义和注册自定义函数
    //定义函数 自己编写匿名函数
    //注册函数：spark.udf.register()
    spark.udf.register("strLen",(str:String)=>str.length())   //这里写的是求字符串长度

    //使用自定义函数
    spark.sql("select name,strLen(name) from names")
        .show()
//      .collect().foreach(println)
  }
}
