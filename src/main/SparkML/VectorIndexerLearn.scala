import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * VectorIndexer类来解决向量数据集中的类别性特征转换
  * @author xjh 2018.10.26
  */
object VectorIndexerLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //读入一个数据集，然后使用VectorIndexer训练出模型，来决定哪些特征需要被作为类别特征，将类别特征转换为索引，这里设置maxCategories为2，
    // 即只有种类小于2的特征才被认为是类别型特征，否则被认为是连续型特征：
    val data=Seq(
      Vectors.dense(-1.0,1.0,1.0),
      Vectors.dense(-1.0,3.0,1.0),
      Vectors.dense(0.0,5.0,1.0)
    )
    val df=spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val indexer=new VectorIndexer().setInputCol("features").setOutputCol("indexed")
      .setMaxCategories(2)

    val indexerModel=indexer.fit(df)

    //可以通过VectorIndexerModel的categoryMaps成员来获得被转换的特征及其映射，
    // 这里可以看到共有两个特征被转换，分别是0号和2号。
    val categoricalFeatures=indexerModel.categoryMaps.keys.toSet
    categoricalFeatures.foreach(println)    //这里的输出值为 0 2

    //0号特征只有-1，0两种取值，分别被映射成0，1，而2号特征只有1种取值，被映射成0
    val indexed=indexerModel.transform(df)
    indexed.show(false)
  }
}
