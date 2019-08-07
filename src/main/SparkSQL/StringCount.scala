import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义类
  * 统计一个字符串出现的次数
  *@author xjh 2018.12.09
  */
class StringCount extends UserDefinedAggregateFunction{ //继承UDAF类
  //inputSchema,指的是输入数据的类型
  override def inputSchema: StructType = StructType(Array(StructField("str",StringType,true)))
  //bufferSchema,指的是中间聚合时，所处理的数据类型
  override def bufferSchema: StructType = StructType(Array(StructField("count",IntegerType,true)))
  //dataType,返回值类型
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true
  //为每个分组的数据进行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0
  }
  //每个分组，有新的值进来的时候 如何让进行分组对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0)+1
  }
  //因为Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合 也就是update操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  //evaluate,指的是一个分组的聚合值 ，如何通过中间的缓存聚合值 最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
