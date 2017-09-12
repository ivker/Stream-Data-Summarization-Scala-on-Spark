package HierarchyStream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object HS_Cov {

  /**
   * Get the start layer and the size by batch_id
   * param batch:batch id,start with 0 ,end with B+K-2/e.g 0...13
   * param K:start with 1 /e.g 5
   * param B:input batch size,start with 1 /e.g 10
   * return (start,length):It's the index of circle ,start with 0 to K-1
   * e.g (3,5) means: [3 2 1 0 (K-1) (K-2)]
   */
  def GetLayerSize(batch: Int, K: Int, B: Int): (Int, Int, Int) = {
    var start = 0
    var length = 0
    var layer = 0
    if (batch > (K - 2) && batch < B) { //e.g 4...9 
      start = batch % K //e.g  4 % 5 = 4 / 9 % 5 = 4
      length = K
      layer = 0
    } else if (batch < (K - 1)) { //e.g 0...3
      start = batch //e.g 0...3
      length = batch + 1
      layer = 0
    } else { //e.g  10...14
      start = (B - 1) % K //e.g  9 % 5 = 4 / 9 % 5 = 4
      length = K - batch + B - 1 //e.g 5 - 10 + 10 - 1 = 4 / 5 - 13 + 10 - 1 = 1 
      layer = batch - B + 1 //e.g 10 - 10 + 1 = 1 / 13 - 10 +  1 = 4
    }
    return (start, length, layer)
  }

  /**
   * the batch(b) in which layer
   */
  def BatchIdToLayerId(b: Int, CurrentBatch: Int): Int = {
    return CurrentBatch - b
  }

  def main(args: Array[String]) {

    //println("Start stamp:" + stamp)
    if (args.length != 5) {
      println("args:<K> <BatchCount> <AllGain> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("HierarchyStream-Coverage")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt //start with 1
    val B = args(1) toInt //Total batch numbers,start with 1
    val AllGain = args(2).toDouble
    val inputPath = args(3)
    val outputPath = args(4)
    var i = 0; //current batch number,start with 0 ... B - 1
    var sumZ = new sumZRCov(K);

    //var DataBlock = new Array[RDD[(Int, (SumZ_Object, Double))]](K); //K batch in memory
    //Not All Layer(DataBlock) is useful,It Record by Bellow two args
    var s_Start = 0; //Data Start Index
    var s_Length = 0; //Data Block size
    var l_Start = 0; //Layer Start Index

    var res = ""
    //2.Batch dealing data
    while (i < B + K - 1) { //total batch = B + K 
      //Test
      println("------------ Batch:" + i + " ------------")
      //println("Round Start stamp:" + stamp)
      //2.1 Get this Round Information
      val tmp = GetLayerSize(i, K, B)
      s_Start = tmp._1
      s_Length = tmp._2
      l_Start = tmp._3
      //Test:
      //println("Start/Length/layerStart:" + s_Start + "/" + s_Length + "/" + l_Start)

      //2.2 Replace the DataBlock(X) with new input & persist it
      val orgData = sc.textFile(inputPath + i)

      //2.3 Add layer's keeper & Left to DataBlock And Join With PerC_f 
      var j = s_Start //From start layer to End
      var l = l_Start //layer index
      var c = 0 //Size Counter
      var Keeper = Array[(SumZ_Object, Double)]()
      var Left = Array[(SumZ_Object, Double)]()
      while (c < s_Length) {
        //1.Keeper
        val keeper = sumZ.Get(l)
        Keeper = Keeper ++ Array((keeper, Gain.GOOoutF(keeper, sumZ.GetC_f(l))))
        //2.Left
        val left = sumZ.GetLeft(l)
        Left = Left ++ Array((left, Gain.GOOoutF(left, sumZ.GetC_f(l))))
        
        l += 1 //Next layer
        c += 1
      }

      //2.4 Map NewData 
      val finalData = orgData.map(x =>
        {
          //1) String To Object
          val (batchid, o) = StringToBatchObject_Cov(x)
          //2) Min Distance
          val minDis = Gain.GOOoutF(o, sumZ.GetC_f(BatchIdToLayerId(batchid, i)))

          (batchid, (o, minDis))
        })

      //2.5 Parallel Dealing
      val keeper = finalData.reduceByKey(BestInBatch)
      //println("SumZ Start stamp:" + stamp)

      //2.6 Update keeper to sumZ
      val ns = SumZDescCov(keeper.take(s_Length), Keeper, Left)
      //Test
      //println("new sumZ size:" + ns.size)

      //2.7update sumZ and C_f
      sumZ.UpdateSumZ(ns, Math.max(0, i - B + 1))

      //2.8 next batch
      i += 1
      //println("Round End stamp:" + stamp)
      //3.Output Result
      res = "HierarchyStream algorithm Coverage : " + Gain.GOO(sumZ.GetC_f(Math.min(i, K))) / AllGain * 100 + "%\n";
      System.out.print(res);

    }
    //println("SumZ End stamp:" + stamp)
    //3.Save Result
    sc.parallelize(Seq(res)).saveAsTextFile(outputPath + "HS" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}