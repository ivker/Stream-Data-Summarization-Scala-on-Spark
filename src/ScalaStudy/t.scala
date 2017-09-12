package ScalaStudy

import Common._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._

object t {

  val updateFunc = (a: Seq[Int], b: Option[Int]) => {
    b
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    var tt = Seq[(Int, SumZ_AttList)]()
    var sa = new SumZ_AttList()
    var i = 0
    while (i < 4000) {
      sa.Add(new SumZ_Att(i, 0))
      i += 1
    }
    tt = tt :+ (999, sa)
    while (true) {
      var PerG_f = sc.parallelize(tt)
      PerG_f.collect()
    }

    sc.stop()
  }

}