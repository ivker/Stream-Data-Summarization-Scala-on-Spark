package RandomTop

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object RT_Cov {

  def main(args: Array[String]) {

    if (args.length != 4) {
      println("args: <K> <allGain> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("RandomTop-Coverage")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt; //start with 1
    val AllGain = args(1).toDouble
    val inputPath = args(2)
    val outputPath = args(3)
    var sumZ = new sumZ;
    var res = ""
    val data = sc.textFile(inputPath).map(StringToBatchObject_Cov)
    val s = data.takeSample(true, K, 569)
    var i = 0
    while (i < K) {
      sumZ.Add(s(i)._2)
      i += 1
    }

    // coverage
    res = "RandomTop algorithm Coverage: " + Gain.GOS(sumZ) / AllGain * 100 + "%\n";
    System.out.println(res)

    //3.Output Result
    sc.parallelize(res).saveAsTextFile(outputPath + "RT" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}