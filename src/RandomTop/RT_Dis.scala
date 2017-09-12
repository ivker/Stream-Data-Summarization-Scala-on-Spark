package RandomTop

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object RT_Dis {

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("args: <K> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("RandomTop-Distance")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt; //start with 1
    val inputPath = args(1)
    val outputPath = args(2)
    var sumZ = new sumZ;

    var res = "";
    val data = sc.textFile(inputPath).map(StringToBatchObject_Dis)
    val s = data.takeSample(true, K, 569)
    var i = 0
    while (i < K) {
      sumZ.Add(s(i)._2)
      i += 1
    }

    //result
    res = "RandomTop algorithm SumZ Distance Lost: " + Gain.MinDisofSumZ(sumZ) + "\nthe SumZ is : " + sumZ.s_ids + "\n"
    System.out.println(res)

    //3.Output Result
    sc.parallelize(res).saveAsTextFile(outputPath + "RT" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}