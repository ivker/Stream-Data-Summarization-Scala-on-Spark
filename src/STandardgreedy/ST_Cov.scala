package STandardgreedy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object ST_Cov {
  def main(args: Array[String]) {
    if (args.length != 5) {
      println("args:<K> <batchNumber> <ALLGain> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("STandardgreedy-Coverage")
    val sc = new SparkContext(conf)

    //1.initial method
    val K = args(0) toInt
    val B = args(1) toInt
    val AllGain = args(2).toDouble
    val inputPath = args(3)
    val outputPath = args(4)
    var sumZ = new sumZ;
    var res = ""
    //2.Load Data & Convert to SumZ_Object
    val orgData = sc.textFile(inputPath).map(StringToBatchObject_Cov)
    val BatchData = orgData.map(x => x._2).collect()

    //sumZ.UpdateC_fGain() //initial sumZ
    //3.Dealing Data
    var i = 0
    while (i < K) { //the size of SumZ is k
      //Test
      println("------------ Batch:" + i + " ------------")
      //1.initial Data
      var maxObject = BatchData(0)
      var maxGain = Gain.GOOoutF(maxObject, Gain.GetCoverageFeather(sumZ))

      var j = 1
      while (j < BatchData.length) {
        val gain = Gain.GOOoutF(BatchData(j), Gain.GetCoverageFeather(sumZ))
        if (gain > maxGain) {
          maxObject = BatchData(j)
          maxGain = gain
        }
        j += 1
      }

      //save result
      sumZ.Add(maxObject)
      //sumZ.UpdateC_fGain()

      i += 1 //next round
    }
    //Result
    res = "StandardGreedy algorithm Coverage:" + Gain.GOS(sumZ) + "\n";
    System.out.print(res);

    //4.output result & save
    sc.parallelize(res).saveAsTextFile(outputPath + "StandardG" + SaveStamp)

    sc.stop()
  }
}