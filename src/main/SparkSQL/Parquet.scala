import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  * 以编程方式加载Parquet数据
  * @author xjh 2018.11.29
  */
object Parquet {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Parquet").setMaster("local")
    val spark=SparkSession.builder().appName("Parquet").config(conf).getOrCreate()
    import spark.implicits._
    val peopleDF=spark.read.json("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\" +
      "spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
    //数据框可以保存为Parquet文件，以维护模式信息
    peopleDF.write.parquet("people.parquet")
    //读取上面创建的parquet文件
    // parquet文件是自描述的，所以模式是保留的
    // 加载parquet文件的结果也是一个DataFrame
    val parquetFileDF=spark.read.parquet("people.parquet")
    //Parquet文件还可以用于创建临时视图，然后在SQL语句中使用
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val nameDF=spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    nameDF.map(attribute=>"Name: "+attribute(0)).show()
    peopleDF.write.parquet("file:///D:\\softInstall\\BigData\\spark-2.3.1-bin-hadoop2.6\\" +
      "spark-2.3.1-bin-hadoop2.6\\examples\\src\\main\\resources\\newpeople.parquet")
  }
}
