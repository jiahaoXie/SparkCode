import org.apache.spark.{SparkConf, SparkContext}

/**
  * 常见的transformation转换操作:map filter groupByKey reduceByKey join
  * （记住 transformation操作满足lazy特性的，只有程序遇到action操作才会从前面的转换操作执行真正的运算）
  * @author xjh 2018.11.19
  */
object Transformation {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Transformation").setMaster("local")
    val sc=new SparkContext(conf)
    val numbers=Array(1,2,3,4,5,6,7,8,9,10)

    //1.利用map实现集合中所有元素*2
//    val n=numbers.map(num=>num*2)
//    for (i <- n)
//      print(i+" ")
//    println()

    //2.利用filter过滤 得到其中的偶数
    val evenNum=numbers.filter(num=>num%2==0)
    for (i<-evenNum)
      print(i+" ")

    //3.利用groupByKey进行分组
    val scoreList=Array(Tuple2("class1",90),Tuple2("class2",75),
      Tuple2("class1",92),Tuple2("class2",60))
    val scores=sc.parallelize(scoreList,2)
    val groupScores=scores.groupByKey()
    groupScores.foreach(score=>{
      println(score._1)
      score._2.foreach(singgleScore=>println(singgleScore))
      println("============================================")
    })

    //4.；利用reduceByKey统计每个班级的总分
//    val totalScore=scores.reduceByKey(_+_)
//    totalScore.foreach(classScore=>println(classScore._1+": "+classScore._2))

    //5.利用sortByKey按照成绩及进行排序
//    val scoreL=Array(Tuple2(72,"leo"),Tuple2(72,"xjh"),Tuple2(60,"mayy"),
//      Tuple2(60,"lexo"),Tuple2(92,"xjhpo"),Tuple2(56,"nbvmayy"))
//    val scores=sc.parallelize(scoreL,4)
//    val scored=scores.sortByKey(false)  //默认升序，加false变为降序
//    scored.foreach(classScore=>println(classScore._1+": "+classScore._2))

    //利用join(关联)打印每个人的成绩
//    val studentList=Array(
//      Tuple2(1,"leo"),Tuple2(2,"xjh"),Tuple2(3,"kobe")
//    )
//    val scoreList=Array(
//      Tuple2(1,100),Tuple2(2,90),Tuple2(3,80)
//    )
//    val student=sc.parallelize(studentList)
//    val score=sc.parallelize(scoreList)
//    val studentScores=student.join(score)
//    studentScores.foreach(studentScore=>{
//      println("student id："+studentScore._1)
//      println("student name: "+studentScore._2._1)
//      println("student score: "+studentScore._2._2)
//      println("===================")
//    })
  }
}
