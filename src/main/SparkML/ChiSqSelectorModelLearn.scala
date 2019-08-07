import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 特征选取–卡方选择器
  * @author xjh 2018.10.26
  */
object ChiSqSelectorModelLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local") //本地测试
      .appName("test ML").enableHiveSupport()
      .getOrCreate() //有就获取 取则创建
    spark.sparkContext.setCheckpointDir("D:\\ideaCode\\MLTest") //设置文件读取 这里设置一个文件夹路径

    //1.创造实验数据，这是一个具有三个样本，四个特征维度的数据集，标签有1，0两种，我们将在此数据集上进行卡方选择
    val df=spark.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
      (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
      (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0)
    )).toDF("id","features","label")
    df.show()

    //2.用卡方选择进行特征选择器的训练，为了观察地更明显，我们设置只选择和标签关联性最强的一个特征（可以通过setNumTopFeatures(..)方法进行设置）
    val selector=new ChiSqSelector().setNumTopFeatures(1).setFeaturesCol("features")
      .setLabelCol("label").setOutputCol("selected-feature")

    //3.用训练出的模型对原数据集进行处理，显示结果可以看到 第三列特征被选出作为最有用的特征列
    val selector_model=selector.fit(df)
    val result=selector_model.transform(df)
    result.show(false)
  }
}
