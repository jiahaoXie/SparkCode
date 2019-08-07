import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * CountVectorizer旨在通过计数来将一个文档转换为向量。
  * @author xjh 2018.10.25
  */
object CountVectorizerLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.创建DataFrame(其包含id和words两列，可以看成是一个包含两个文档的迷你语料库)
    val df=spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id","words")

    //2.通过CountVectorizer设定超参数，训练一个CountVectorizerModel，这里设定词汇表的最大量为3，
    // 设定词汇表中的词至少要在2个文档中出现过，以过滤那些偶然出现的词汇。
    val cvModel=new CountVectorizer().setInputCol("words").setOutputCol("features")
      .setVocabSize(3).setMinDF(2).fit(df);
    //在训练结束后，可以通过CountVectorizerModel的vocabulary成员获得到模型的词汇表
    cvModel.vocabulary.foreach(print)   //得到输出结果:bac
    //使用这一模型对DataFrame进行变换，可以得到文档的向量化表示：
    cvModel.transform(df).show(false)

    //和其他Transformer不同，CountVectorizerModel可以通过指定一个先验词汇表来直接生成
    val cvm=new CountVectorizerModel(Array("a","b","c"))
      .setInputCol("words").setOutputCol("features")
    cvm.transform(df).select("features").take(3).foreach(println )
  }
}
