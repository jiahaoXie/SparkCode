import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现二次排序：
    *1、按照文件中的第一列排序。
     2、如果第一列相同，则按照第二列排序。
  * @author xjh 2018.11.20
  */
/**
  * 首先自定定义用于实现二次排序的类
  * @param first
  * @param second
  */
class SecondSort(val first:Int,val second:Int)
  extends Ordered[SecondSort] with Serializable{
  override def compare(that: SecondSort): Int = {
    if (this.first==that.first){
      this.second-that.second
    }else this.first-that.first
  }
}
object SecondSortKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SecondSortKey").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("D:\\ideaCode\\spark_wordCount\\sort.txt")
    val pairs=lines.map{line=>(
      new SecondSort(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line
    )}  //将其映射为pairRDD
    val sortPairs=pairs.sortByKey()   //默认升序
    val sortedLines=sortPairs.map(sortPair=>sortPair._2)
      //取tuple得第二列元素(也就是有效值) 因为sortPair._1是 hashCode值
//    sortPairs.foreach(println)
    sortedLines.foreach(println)
  }
}
