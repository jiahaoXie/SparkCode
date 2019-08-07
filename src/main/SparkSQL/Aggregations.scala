import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 非类型用户定义的聚合函数
  * @author xjh 2018.11.28
  * 本案例是用户必须扩展UserDefinedAggregateFunction 抽象类以实现自定义无类型聚合函数。
  * 得到用户定义的平均值
  */
object MyAverage extends UserDefinedAggregateFunction{
  //此聚合函数的输入参数的数据类型
  def inputSchema:StructType=StructType(StructField("inputColumn",LongType)::Nil)

  //聚合缓冲区中值的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
  }
  //返回值的数据类型
  def dataType:DataType=DoubleType

  //这个函数是否总是对相同的输入返回相同的输出
  override def deterministic: Boolean = true

  //初始化给定的聚合缓冲区。缓冲区本身是一个“行”，除了
  // 检索索引值(如get()、getBoolean())等标准方法之外，还提供
  // 更新其值的机会。注意，缓冲区中的数组和映射仍然是不可变的。
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }

  //使用来自“input”的新输入数据更新给定的聚合缓冲区“缓冲区”
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getLong(1)+1
    }
  }

  //合并两个聚合缓冲区，并将更新后的缓冲区值存储回“buffer1”
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  //计算得到最后结果
  override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble/buffer.getLong(1)

}
object Aggregations{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Aggregations").setMaster("local")
    val spark=SparkSession.builder().appName("Aggregations").config(conf).getOrCreate()
    //注册函数来访问它
    spark.udf.register("myAverage",MyAverage)
    val df=spark.read
      .json("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\employees.json")
    df.createOrReplaceTempView("employees")
    df.show()   //输出表格中的数据
    /**
    +-------+------+
    |   name|salary|
    +-------+------+
    |Michael|  3000|
      |   Andy|  4500|
      | Justin|  3500|
      |  Berta|  4000|
      +-------+------+
      **/

    val result=spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show() //输出平均分
    /**
      * +--------------+
        |average_salary|
        +--------------+
        |        3750.0|
        +--------------+
      */
  }
}
