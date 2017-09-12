package GreeDi

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

import java.io._, java.nio.channels._, java.nio._

object GD_Dis {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("args:<K> <B> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("GreeDi Distance")
    val sc = new SparkContext(conf)

    //1.initial method
    val K = args(0) toInt
    val B = args(1) toInt //Total batch numbers,start with 1
    val inputPath = args(2)
    val outputPath = args(3)
    var s = new sumZ; //final sumZ

    var i = 0 //current batch number,start with 0 ... B - 1
    var res = ""
    //2.Batch dealing data
    while (i < B) { //total batch = B
      //2.1 Load PerData with new input & persist it
      val orgData = sc.textFile(inputPath + i).map(StringToBatchObject_Dis)
      val BatchData = orgData.map(x => x._2).collect()

      //2.2 Dealing Batch or Not
      println("------------ Batch:" + i + " ------------")

      var newSumZ = new sumZ;
      //newSumZ.UpdateD_fGain() //initial sumZ
      //2.3 K round
      var l = 0; //the size of SumZ is k:l round
      while (l < K) {
        //1.initial Data
        var minObject = BatchData(0)
        var minDis = Gain.MinDisWithF(minObject, Gain.GetDistanceFeather(newSumZ))

        var j = 1
        while (j < BatchData.length) {
          val gain = Gain.MinDisWithF(BatchData(j), Gain.GetDistanceFeather(newSumZ))
          //System.out.println(j + " gain:" + gain + " / min gain:" + minDis)
          if (gain < minDis) {
            minObject = BatchData(j)
            minDis = gain
          }
          j += 1
        }
        j = 0
        while (j < s.Size()) {
          val gain = Gain.MinDisWithF(s.Get(j), Gain.GetDistanceFeather(newSumZ))
          if (gain < minDis) {
            minObject = s.Get(j)
            minDis = gain
          }
          j += 1
        }

        //save result
        //System.out.println("Add gain:" + minDis)
        newSumZ.Add(minObject)
        //newSumZ.UpdateD_fGain()
        //System.out.println(" l:" + l + " / " + newSumZ.g + " / min gain:" + minDis)
        l += 1 //next round
      }

      //2.4 replace s with newSumZ
      s = newSumZ

      //2.5 Result
      res = "GreeDi algorithm SumZ Distance Lost:" + Gain.MinDisofSumZ(s) + "\nthe SumZ is : " + s.s_ids() + "\n"
      System.out.print(res);

      i += 1 //next batch
    }

    //4.output result & save
    sc.parallelize(res).saveAsTextFile(outputPath + "GD" + SaveStamp)
    System.out.print("Done!");
    sc.stop()
  }
}
