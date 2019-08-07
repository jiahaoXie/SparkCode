import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform以及实时黑名单过滤案例
  * @author xjh 2018.12.14
  */
object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UpdateStateByKeyToWordCount").setMaster("local[4]")
    val ssc=new StreamingContext(conf,Seconds(10))    //创建StreamingContext对象
    // 用户对我们的网站上的广告可以进行点击
    // 点击之后，是不是要进行实时计费，点一下，算一次钱
    // 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
    // 只要是黑名单中的用户点击的广告，我们就给过滤掉

    // 先做一份模拟的黑名单RDD
    val blackList=Array(
      ("tom",true),("jack",true)
    )
    val blackListRDD=ssc.sparkContext.parallelize(blackList,4)
    //实时流输入源 这采用socketTextStream  虚拟机上输入 nc -lk 9999
    val adsDStream=ssc.socketTextStream("msiPC",9999)
    // 所以，要先对输入的数据，进行一下转换操作，变成，(username, date username)
    // 以便于，后面对每个batch RDD，与定义好的黑名单RDD进行join操作
    val userAdsDStream=adsDStream.map(ads=>(ads.split(" ")(1),ads))
    //执行transform操作了，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作
    // 实时进行黑名单过滤
    val validAdsClickLogDStream = userAdsDStream.transform(userAdsClickLogRDD => {
      // 这里为什么用左外连接？
      // 因为，并不是每个用户都存在于黑名单中的
      // 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到 就给丢弃掉了
      // 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到
      // 也还是会被保存下来的
      val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD)
      //连接之后，执行fliter算子
      //这里的tuple，就是每个用户，对应的访问日志，和在黑名单中的状态
      val filteredRDD = joinedRDD.filter(tuple => {
        if(tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      //过滤之后 只剩下没有被黑名单过滤的用户点击事件了
      //进行map操作 转换成为想要的格式
      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD
    })
    validAdsClickLogDStream.print()   //打印输出数据信息
    ssc.start()
    ssc.awaitTermination()
  }
}
