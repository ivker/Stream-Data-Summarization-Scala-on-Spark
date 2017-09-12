package SieveStream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object SS_Cov {

  def main(args: Array[String]) {

    if (args.length != 8) {
      println("args: <K> <BatchNum> <e> <Covmin> <Covmax> <allGain> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("SieveStream")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt //start with 1
    val B = args(1) toInt //Total batch numbers,start with 1
    val e = args(2).toDouble // (1+e)^i
    val min = args(3).toInt
    val max = args(4).toInt
    val AllGain = args(5).toDouble
    val inputPath = args(6)
    val outputPath = args(7)
    var i = 0; //current batch number,start with 0 ... B - 1
    var maxGain = 0;
    //Dealing Data Format
    //(siever_id,(OPT,sumZ,K,Object))-(key,value)
    var sievers = new SumZ_SieverListCov(K, e, min, max)
    //Test
    //println("sievers's size:" + sievers.Size())

    var res = "";
    //2.Batch dealing data
    while (i < B) { //total batch = B
      //Test
      println("------------ Batch:" + i + " ------------")
      //-----------NOTE:SUMZ UPDATE METHOD HAVE NOT DONE YET--------

      //2.1load data
      val orgData = sc.textFile(inputPath + i)

      //2.2 Map to All Siever
      val FinalData = orgData.flatMap(x => {
        //1) String To Object
        val o = StringToBatchObject_Cov(x)

        //2)flatMap  
        val size = sievers.Size()
        var res = new Array[(Int, (SumZ_Object, SumZ_SieverCov))](size)
        var i = 0
        while (i < size) {
          res(i) = (i, (o._2, sievers.Get(i)))
          i += 1
        }
        res
      })

      //2.3 Parallel Dealing
      val Res = FinalData.reduceByKey(SieveDataCov)

      //2.4 Update sumZ
      sievers.UpdateSieverList(ToSieverListCov(Res.collect))
      //Test
      //var j = 0
      //while (j < sievers.Size()) {
      //  System.out.println(sievers.Get(j).s.Size())
      //  j += 1
      //}

      //2.7 Show Result
      sievers.GetBest

      //result 
      res = "SieveStream algorithm Coverage : " + sievers.BestGain * 100 / AllGain + "% \n";
      System.out.print(res);

      //2.8 next batch
      i += 1
    }

    //3.Output Result
    sc.parallelize(res).saveAsTextFile(outputPath + "SS" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}