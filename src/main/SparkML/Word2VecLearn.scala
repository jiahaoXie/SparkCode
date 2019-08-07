import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
  *Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，
  * 它可以计算每个单词在其给定语料库环境下的 分布式词向量（Distributed Representation，亦直接被称为词向量）。
  * @author xjh 2018.10.25
  */
object Word2VecLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //我们首先用一组文档，其中一个词语序列代表一个文档。
    // 对于每一个文档，我们将其转换为一个特征向量。此特征向量可以被传递到一个学习算法。
    //1.导入Word2Vec所需要的包，并创建三个词语序列，每个代表一个文档：
    val documentDF=spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    //2.新建一个Word2Vec，显然，它是一个Estimator，设置相应的超参数，这里设置特征向量的维度为3
    val word2Vec=new Word2Vec().setInputCol("text").setOutputCol("result")
      .setVectorSize(3).setMinCount(0)

    //3.读入训练数据 用fit()方法生成一个Word2VecModel
    val model=word2Vec.fit(documentDF)
    //4.利用word2VecModel把文档转变成特征向量
    val result=model.transform(documentDF)
    result.select("result").take(3).foreach(println)
    //显示结果 文档变成一个3维的特征向量
  }
}
