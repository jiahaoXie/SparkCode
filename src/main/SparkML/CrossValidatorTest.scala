import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/**
  * 由MLPipelines改进得到
  * 使用交叉验证
  *这个正确率 低，仅仅作为测试运行样例
  * @author xjh 2018.10.25
  */
object CrossValidatorTest {
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
    training.show(false) //完整显示内容，不截断

    //2.要定义 Pipeline 中的各个工作流阶段PipelineStage，包括转换器和评估器，具体的，包含tokenizer, hashingTF和lr三个步骤。
    // 参数设置：tokenizer、hashingTF、lr
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    //按照具体的处理逻辑 有序的组织PipelineStages并创建一个Pipeline
    //现在构建的Pipeline本质上是一个Estimator，在它的fit（）方法运行之后，它将产生一个PipelineModel，它是一个Transformer。
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    //上述和之前的一样

    //3.建立网络搜索
    val paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01)).build()

    //4.建立一个交叉验证的评估器 设置评估器的参数
    val cv = new CrossValidator().setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    //5.运行交叉验证评估器，得到zui'最佳参数集的模型
    val cvModel = cv.fit(training)
    //6.测试数据
    val test=spark.sqlContext.createDataFrame(Seq(
      Test(1L, "You love me"),
      Test(2L, "Your happy passer-by all knows, my distressed there is no place hides"),
      Test(3L, "You may be out of my sight, but never out of my mind"),
      Test(4L, "Do you like me")
    )).toDF()
    test.show(false)

    //7.模型预测
    cvModel.transform(test).select("id","text","probability","prediction").show(false)
  }
}
