import org.apache.spark.{SparkConf, SparkContext}

/**
  * action常用操作 reduce collect count take
  * @author xjh 2018.11.20
  */
object Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Action").setMaster("local")
    val sc=new SparkContext(conf)
    val numArray=Array(1,2,3,4,5,6,7,8,9)
    val numbers=sc.parallelize(numArray)
    val sum=numbers.reduce(_+_) //利用reduce操作实现累加
    println("sum: "+sum)

    val doubleNum=numbers.map(num=>num*2)
    val doubleNumArray=doubleNum.collect()  //通过collect返回得到所有元素
    for (num<-doubleNumArray)
      print(num+" ")

    val count=numbers.count() //利用count得到数量
    println("count: "+count)

    val top3Num=numbers.take(3)   //take取前3个
    for (num<-top3Num)
      print(num+" ")

    val studentList=Array(
      Tuple2("class1","xjh"),Tuple2("class2","cdsacd"),Tuple2("class3","jiahao"),
      Tuple2("class1","kobe"),Tuple2("class2","James"),Tuple2("class1","KD")
    )
    val students=sc.parallelize(studentList)


    //countByKey
    val studentCount=students.countByKey()
    println(studentCount)   //输出结果：Map(class3 -> 1, class1 -> 3, class2 -> 2)

  }
}
