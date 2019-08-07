import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  *特征抽取 词频-逆向文件频率（TF-IDF）
  *@author xjh 2018.10.25
  */
object TF_IDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.创建一个简单的DataFrame，每个句子 代表一个文档
    val sentenceData=spark.createDataFrame(Seq(
      (0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label","sentence")

    //2。利用tokenizer对句子进行分词
    val tokenizer=new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData=tokenizer.transform(sentenceData)
    wordsData.show(false) //显示 句子分词后的结果

    //3.得到分词后的文档序列后，即可使用HashingTF的transform()方法把句子哈希成特征向量，
    // 这里设置哈希表的桶数为2000。
    val hashingTF=new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizeData=hashingTF.transform(wordsData)
    featurizeData.select("rawFeatures").show(false)
    //分词序列被变换成一个稀疏特征向量，其中每个单词都被散列成了一个不同的索引值，
    // 特征向量在某一维度上的值即该词汇在文档中出现的次数。

    //4.使用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力，
    // IDF是一个Estimator，调用fit()方法并将词频向量传入，即产生一个IDFModel
    val idf=new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel=idf.fit(featurizeData)
    //IDFModel是一个Transformer，调用它的transform()方法，即可得到每一个单词对应的TF-IDF度量值。
    val rescaledData=idfModel.transform(featurizeData)
    rescaledData.select("features","label").take(3).foreach(println)
    //输出结果看到：特征向量已经被其在语料库中出现的总次数进行了修正，
    // 通过TF-IDF得到的特征向量，在接下来可以被应用到相关的机器学习方法中。
  }
}
