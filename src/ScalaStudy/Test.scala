package ScalaStudy
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
  
object Test {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    var ha=HashMap("we"->"er","sd"->"fg","sx"->"cv")
    for(x <- ha.keys.toArray)
    println(x)
    val tmp=ha.keys.toArray
    var arr=ArrayBuffer[String]()
    val res=arr++=tmp
 
    
    
    val inputPath = args(0)
    val orgData = sc.textFile(inputPath + "0")
    val out = orgData.collect()
    var i = 0
    var count = 0;
    while (i < out.size) {
      var j = 0
      while (j < out(1).size * 500) {
        count += 1
        j += 1
      }
      println("count:" + count)
      i += 1
    }
    //4. Ends

    sc.stop()
  }
}