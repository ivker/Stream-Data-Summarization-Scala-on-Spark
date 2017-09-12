package ScalaStudy
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object Test2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val orgData = sc.textFile(inputPath + "0")
    val out = orgData.map(x => {
      var j = 0
      while (j < x.size * 500) {
        j += 1
      }
      j
    }).collect()
    var count = 0;
    var i = 0
    while (i < out.size) {
      println("out(i):" + out(i))
      count += out(i)
      i += 1
    }
    //4. Ends
    println("count:" + count)
    sc.stop()
  }
}