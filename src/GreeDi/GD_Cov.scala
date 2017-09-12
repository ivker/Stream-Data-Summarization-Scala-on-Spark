package GreeDi

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

import java.io._, java.nio.channels._, java.nio._

object GD_Cov {
  def main(args: Array[String]) {
    if (args.length != 5) {
      println("args:<K> <B> <AllGain> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("GreeDi-Coverage")
    val sc = new SparkContext(conf)

    //1.initial method
    val K = args(0) toInt
    val B = args(1) toInt //Total batch numbers,start with 1
    val AllGain = args(2).toDouble
    val inputPath = args(3)
    val outputPath = args(4)
    var s = new sumZ; //final sumZ

    var i = 0 //current batch number,start with 0 ... B - 1
    var res = ""
    //2.Batch dealing data
    while (i < B) { //total batch = B
      //2.1 Load PerData with new input & persist it
      val orgData = sc.textFile(inputPath + i).map(StringToBatchObject_Cov)
      val BatchData = orgData.map(x => x._2).collect()

      //2.2 Dealing Batch or Not
      println("------------ Batch:" + i + " ------------")

      var newSumZ = new sumZ;
      //newSumZ.UpdateC_fGain() //initial sumZ
      //2.3 K round
      var l = 0; //the size of SumZ is k:l round
      while (l < K) {
        //1.initial Data
        var maxObject = BatchData(0)
        var maxGain = Gain.GOOoutF(maxObject, Gain.GetCoverageFeather(newSumZ))

        var j = 1
        while (j < BatchData.length) {
          val gain = Gain.GOOoutF(BatchData(j), Gain.GetCoverageFeather(newSumZ))
          if (gain > maxGain) {
            maxObject = BatchData(j)
            maxGain = gain
          }
          j += 1
        }
        j = 0
        while (j < s.Size()) {
          val gain = Gain.GOOoutF(s.Get(j), Gain.GetCoverageFeather(newSumZ))
          if (gain > maxGain) {
            maxObject = s.Get(j)
            maxGain = gain
          }
          j += 1
        }

        //save result
        newSumZ.Add(maxObject)
        //newSumZ.UpdateC_fGain()

        l += 1 //next round
      }

      //2.4 replace s with newSumZ
      s = newSumZ

      //2.5 Result
      res = "GreeDi algorithm SumZ Gain:" + Gain.GOS(s) / AllGain * 100 + "%\n";
      System.out.print(res);

      i += 1 //next batch
    }

    //4.output result & save
    sc.parallelize(res).saveAsTextFile(outputPath + "GD" + SaveStamp)
    System.out.print("Done!");
    sc.stop()
  }
}
