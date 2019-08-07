import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/**
  * 特征变换–标签和索引的转化
  *
  * @author xjh 2018.10.26
  */
object StringIndexerLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.创建一个简单的DataFrame,它只包含一个id和一个标签category
    val df1 = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    //2.创建一个StringIndexer对象，设定输入输出列名，其余参数采用默认值，
    // 并对这个DataFrame进行训练，产生StringIndexerModel对象：
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    val model = indexer.fit(df1)

    //3.随后即可利用该对象对DataFrame进行转换操作，
    // StringIndexerModel依次按照出现频率的高低，把字符标签进行了排序
    val indexd1 = model.transform(df1)
    indexd1.show(false)

    //这里作为测试 我们创建新的DataFrame
    //DataFrame中有着模型内未曾出现的标签“d,处理方法是：
    //在模型训练后，可以通过设置setHandleInvalid("skip")来忽略掉那些未出现的标签，这样带有未出现标签的行将直接被过滤掉
    val df2 = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "d")
    )).toDF("id", "category")
    val indexd2 = model.setHandleInvalid("skip").transform(df2)
    indexd2.show()
  }
}
