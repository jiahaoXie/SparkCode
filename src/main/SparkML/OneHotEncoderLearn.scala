import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * OneHotEncoder简单运用
  * @author xjh 2018.10.26
  */
object OneHotEncoderLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.首先创建一个DataFrame，其包含一列类别性特征，
    // 需要注意的是，在使用OneHotEncoder进行转换前，DataFrame需要先使用StringIndexer将原始标签数值化
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c"),
      (6, "d"),
      (7, "d"),
      (8, "d"),
      (9, "d"),
      (10, "e"),
      (11, "e"),
      (12, "e"),
      (13, "e"),
      (14, "e")
    )).toDF("id","category")
    val model=new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
    val indexed=model.transform(df)

    //2.创建OneHotEncoder对象对处理后的DataFrame进行编码，
    // 可以看见，编码后的二进制特征呈稀疏向量形式，与StringIndexer编码的顺序相同
    val encoder=new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
    val encoded=encoder.transform(indexed)
    encoded.show()

  }
}
