import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * IndexToString的作用是把标签索引的一列重新映射回原有的字符型标签
  * @author xjh 2018.10.26
  */
object IndexToStringLearn {
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

    //上述操作 和之前StringIndexr是一样的

    //4.创建IndexToString对象，读取“categoryIndex”上的标签索引，获得原有数据集的字符型标签，然后再输出到“originalCategory”列上。
    // 最后，通过输出“originalCategory”列，可以看到数据集中原有的字符标签。
    val converter=new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")
    val converted=converter.transform(indexd1)
    converted.select("id","originalCategory").show(false)
  }
}
