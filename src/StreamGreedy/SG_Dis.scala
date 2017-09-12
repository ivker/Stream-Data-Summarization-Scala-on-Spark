package StreamGreedy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object SG_Dis {

  var K = 0;
  var sumZ = new sumZ();

  /*
   * (Per)SumZ deal with a new Object
   * 
   * @param oid:if the object's gain is enough,we will save the id of this
   * object
   * 
   * @param o:info of object(o)
   */
  def DealOne(o: SumZ_Object) {
    // 1.choose the best result replace one object in sumZ
    var swapid = SwapEachOne(o);
    // 2.swap the object in sumZ,
    if (swapid != -1) {
      SumZreplace(swapid, o);
    }
  }

  /*
   * Swap Each One object in sumZ with object(o) choose the max gain of swap
   * result
   * 
   * @param o :new object
   * 
   * @return : beat replace object index.if no will return -1;
   */
  def SwapEachOne(o: SumZ_Object): Int = {

    var maxg = Gain.MinDisofSumZ(sumZ);
    var maxid = -1;
    var j = 0
    // traversal all sumZ
    while (j < K) {
      var tsumZ = new sumZ();
      // replace i==j
      var l = 0
      while (l < K) {
        if (l == j)
          tsumZ.Add(o);
        else
          tsumZ.Add(sumZ.Get(l));
        l += 1
      }

      var g = Gain.MinDisofSumZ(tsumZ);
      if (g < maxg) {
        maxg = g;
        maxid = j;
      }
      j += 1
    }

    return maxid;
  }

  /*
   * replace the object(id) in sumZ with object(oid)
   * 
   * @param id: the index of object in sumZ
   * 
   * @param oid: new object's true id.
   * 
   * @param o:the infomation of new object
   */
  def SumZreplace(id: Int, o: SumZ_Object) {
    sumZ.Remove(id);
    sumZ.Add(o);
  }

  def main(args: Array[String]) {

    if (args.length != 4) {
      println("args: <K> <BatchNum> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("StreamGreedy Distance")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    K = args(0) toInt; //start with 1
    val B = args(1) toInt //Total batch numbers,start with 1
    val inputPath = args(2)
    val outputPath = args(3)
    var i = 0 //current batch number,start with 0 ... B - 1
    var j = 0 //Iteration arg
    var res = ""
    // Initial first-k data with empty
    while (j < K) {
      sumZ.Add(new SumZ_Object(-1));
      j += 1
    }

    //2.Batch dealing data
    while (i < B) { //total batch = B
      //Test
      println("------------ Batch:" + i + " ------------")

      //2.1 Load PerData with new input & persist it
      val BatchData = sc.textFile(inputPath + i).map(StringToBatchObject_Dis)
      //Test:PerData(x).saveAsTextFile(outputPath + "T" + i)

      // 2.2 deal all data
      val PerData = BatchData.collect()
      //Test
      //println("PerData Size:" + PerData.size)

      j = 0
      while (j < PerData.size) {
        DealOne(PerData(j)._2)
        j += 1
      }

      //3.Output Result
      res = "StreamGreedy algorithm SumZ Distance Lost: " + Gain.MinDisofSumZ(sumZ) + "\nthe SumZ is : " + sumZ.s_ids + "\n"
      System.out.print(res);

      //2.3 Next Batch
      i += 1
    }

    //3.Output Result
    sc.parallelize(Seq(res)).saveAsTextFile(outputPath + "StreamG" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
  }
}