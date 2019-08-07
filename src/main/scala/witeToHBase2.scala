import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  *spark使用saveAsHadoopDataset写入数据
  * @author xjh 2018.09.25
  */
object witeToHBase2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("witeToHBase2").setMaster("local")
    val sc=new SparkContext(sparkConf)

    val conf=HBaseConfiguration.create()
    //设置zookerper集群地址，也可以通过将hbase-site.xml导入classspath,但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","msiPC")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort","2181")

    val tablename="student"
    //初始化jobConf,TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
    val jobConf=new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tablename)

    val indataRDD=sc.makeRDD(Array("5,Rong,M,26","6,hua,W,27"))  //插入两行数据
    val rdd=indataRDD.map(_.split(',')).map{arr=>{
      //一个put对象就是一行记录，在构造方法中指定主键
      //所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
      //Put.add接收三个参数：列族 列名 数据
      val put=new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))  //info:name列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))  //info:gender列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt)) //info:age列的值

      //转换成RDD(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
