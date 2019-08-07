import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

//封装DataFrame中的数据
case class Love(id: Long, text: String, label: Double)
case class Test(id: Long, text: String)

/**
  * Spark MLlib构建一个机器学习工作流
  * ML Pipelines 管道
  * @author xjh 2018.10.25
  */
object MLPipelines {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.训练样本
    val training = spark.createDataFrame(
      Seq(
        Love(1L, "I love you", 1.0),
        Love(2L, "There is nothing to do", 0.0),
        Love(3L, "Work hard and you will success", 0.0),
        Love(4L, "We love each other", 1.0),
        Love(5L, "Where there is love, there are always wishes", 1.0),
        Love(6L, "I love you not because who you are,but because who I am when I am with you", 1.0),
        Love(7L, "Never frown,even when you are sad,because youn ever know who is falling in love with your smile", 1.0),
        Love(8L, "Whatever is worth doing is worth doing well", 0.0),
        Love(9L, "The hard part isn’t making the decision. It’s living with it", 0.0),
        Love(10L, "Your happy passer-by all knows, my distressed there is no place hides", 0.0),
        Love(11L, "When the whole world is about to rain, let’s make it clear in our heart together", 0.0)
      )
    ).toDF()
    training.show(false)  //完整显示内容，不截断

    //2.要定义 Pipeline 中的各个工作流阶段PipelineStage，包括转换器和评估器，具体的，包含tokenizer, hashingTF和lr三个步骤。
    // 参数设置：tokenizer、hashingTF（）、lr
        //首先使用分解器Tokenizer把句子划分为单个词语。对每一个句子（词袋），我们使用HashingTF将句子转换为特征向量，最后使用IDF重新调整特征向量。
        // 这种转换通常可以提高使用文本特征的性能。
    val tokenizer=new Tokenizer().setInputCol("text").setOutputCol("words")
      //在得到文档集合后，即可用tokenizer对句子进行分词。
    val hashingTF=new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
      //这里设置哈希表的桶数为1000
    val lr=new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    //按照具体的处理逻辑 有序的组织PipelineStages并创建一个Pipeline
    //现在构建的Pipeline本质上是一个Estimator，在它的fit（）方法运行之后，它将产生一个PipelineModel，它是一个Transformer。
    val pipeline=new Pipeline().setStages(Array(tokenizer,hashingTF,lr))

    //3.训练模型
    val model=pipeline.fit(training)    //model的类型是一个PipelineModel，这个管道模型将在测试数据的时候使用

    //4.测试数据
    val test=spark.sqlContext.createDataFrame(Seq(
      Test(1L,"You love me"),
      Test(2L, "Your happy passer-by all knows, my distressed there is no place hides"),
      Test(3L, "You may be out of my sight, but never out of my mind"),
      Test(4L, "Do you like me")
    )).toDF()
    test.show(false)  //完整显示内容

    //5.模型预测
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .show(false)    //本次输出 和之前那次输出的结果 完全一致，正确率不可思议达到100%

    //6.保存 训练方式 pipeline
    pipeline.write.overwrite().save("D:\\ideaCode\\MLTest\\pipelineTest")
    //7.保存 预测模型 model
    model.write.overwrite().save("D:\\ideaCode\\MLTest\\modelTest")
    //8.读取 预测模型 model验证
    val sameModel=PipelineModel.load("D:\\ideaCode\\MLTest\\modelTest")
    //9.读取 预测模型 model验证
    sameModel.transform(test).select("id","text","probability","prediction").show(false)

  }
}
