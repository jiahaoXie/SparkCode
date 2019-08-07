import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 类型安全的用户定义聚合函数
  * @author xjh 2018.11.29
  */
case class Employee(name: String,salary:Long)
case class Average(var sum:Long,var count:Long)
object MyAverage2 extends Aggregator[Employee,Average,Double]{
  //此聚合的值为零。是否满足任意b0 = b的性质
  def zero:Average=Average(0L,0)

  //将两个值组合生成一个新值。为了提高性能，函数可以修改‘buffer’
  // 并返回它，而不是构造一个新的对象
  override def reduce(b: Average, a: Employee): Average = {
    b.sum+=a.salary
    b.count+=1
    b
  }
  //合并两个中间值
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }

  //转换reduction输出
  override def finish(reduction: Average): Double = reduction.sum.toDouble/reduction.count

  //指定中间值类型的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //指定最终输出值类型的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
object Aggregations2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Aggregations2").setMaster("local")
    val spark=SparkSession.builder().appName("Aggregations2").config(conf).getOrCreate()
    import spark.implicits._
    val df=spark.read.json("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\" +
      "spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\employees.json")
    val ds=df.as[Employee]  //运行这一句时 一定记得加上import spark.implicits._ 使其支持类型隐式转换
    ds.show()
    /**
      * +-------+------+
        |   name|salary|
        +-------+------+
        |Michael|  3000|
        |   Andy|  4500|
        | Justin|  3500|
        |  Berta|  4000|
        +-------+------+
      */

    //将函数转换为' TypedColumn '并为其命名
    val averageSalary=MyAverage2.toColumn.name("average_salary")
    val result=ds.select(averageSalary)
    result.show()
    /**
      * +--------------+
        |average_salary|
        +--------------+
        |        3750.0|
        +--------------+
      */
  }
}
