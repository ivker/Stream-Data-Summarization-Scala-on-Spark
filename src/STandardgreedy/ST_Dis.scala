package STandardgreedy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object ST_Dis {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("args:<K> <BatchNum> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("STandardgreedy-Distance")
    val sc = new SparkContext(conf)

    //1.initial method
    val K = args(0) toInt
    val B = args(1) toInt
    val inputPath = args(2)
    val outputPath = args(3)
    var sumZ = new sumZ;
    var res = ""
    //2.Load Data & Convert to SumZ_Object
    val orgData = sc.textFile(inputPath).map(StringToBatchObject_Dis)
    val BatchData = orgData.map(x => x._2).collect()

    sumZ.UpdateD_fGain() //initial sumZ
    //3.Dealing Data
    var i = 0
    while (i < K) { //the size of SumZ is k
      //Test
      println("------------ Batch:" + i + " ------------")
      //1.initial Data
      var minObject = BatchData(0)
      var minDis = Gain.MinDisWithF(minObject, Gain.GetDistanceFeather(sumZ))

      var j = 1
      while (j < BatchData.length) {
        val gain = Gain.MinDisWithF(BatchData(j), Gain.GetDistanceFeather(sumZ))
        if (gain < minDis) {
          minObject = BatchData(j)
          minDis = gain
        }
        j += 1
      }

      //save result
      sumZ.Add(minObject)
      //sumZ.UpdateD_fGain()

      i += 1 //next round
    }
    //Result
    res = "StandardGreedy algorithm SumZ Distance Lost:" + Gain.MinDisofSumZ(sumZ) + "\nthe SumZ is : " + sumZ.s_ids() + "\n"
    System.out.print(res);

    //4.output result & save
    sc.parallelize(res).saveAsTextFile(outputPath + "StandardG" + SaveStamp)

    sc.stop()
  }
}