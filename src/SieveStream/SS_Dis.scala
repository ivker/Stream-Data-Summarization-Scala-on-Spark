package SieveStream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object SS_Dis {

  def main(args: Array[String]) {

    if (args.length != 7) {
      println("args: <K> <BatchNum> <e> <Dismin> <Dismax> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("SieveStream Distance")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt //start with 1
    val B = args(1) toInt //Total batch numbers,start with 1
    val e = args(2).toDouble // (1+e)^i
    val Dismin = args(3).toInt
    val Dismax = args(4).toInt
    val inputPath = args(5)
    val outputPath = args(6)
    var i = 0; //current batch number,start with 0 ... B - 1
    //Dealing Data Format
    //(siever_id,(OPT,sumZ,K,Object))-(key,value)
    var sievers = new SumZ_SieverListDis(K, e, Dismin, Dismax)
    var res = ""
    //2.Batch dealing data
    while (i < B) { //total batch = B
      //Test
      println("------------ Batch:" + i + " ------------")

      //2.1load data
      val orgData = sc.textFile(inputPath + i)

      //2.2 Map to All Siever
      val FinalData = orgData.flatMap(x => {
        //1) String To Object
        val (batchid, o) = StringToBatchObject_Dis(x)

        //2)flatMap   
        val size = sievers.Size()
        var res = new Array[(Int, (SumZ_Object, SumZ_SieverDis))](size)
        var i = 0
        while (i < size) {
          res(i) = (i, (o, sievers.Get(i)))
          i += 1
        }
        res
      })

      //2.3 Parallel Dealing
      val Res = FinalData.reduceByKey(SieveDataDis)

      //2.4 Update sumZ
      sievers.UpdateSieverList(ToSieverListDis(Res.collect()))
      //Test
      //println("Res's size:" + Res.collect().size)

      //2.5 Show Result
      sievers.GetBest
      //result 
      res = "SieveStream algorithm SumZ Distance Lost:" + Gain.MinDisofSumZ(sievers.Best.s) + "\n";
      System.out.print(res);

      //2.8 next batch
      i += 1
    }

    sc.parallelize(res).saveAsTextFile(outputPath + "SS" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}