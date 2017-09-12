package HierarchyStream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import java.util.ArrayList;

import Common._
import Common.Scala.Convert._
import Common.Scala.Transformation._

object HS_Dis_old {

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

  def main(args: Array[String]) {
  /*
    //println("Start stamp:" + stamp)
    if (args.length != 4) {
      println("args:<K> <BatchCount> <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("HierarchyStream-Distance")
    val sc = new SparkContext(conf)

    //1.initial algorithm
    val K = args(0) toInt //start with 1
    val B = args(1) toInt //Total batch numbers,start with 1
    val inputPath = args(2)
    val outputPath = args(3)
    var i = 0; //current batch number,start with 0 ... B - 1
    var sumZ = new sumZRDis(K);
    //Int:batch_index,sumZ_Object:Data,Double:Gain of this Object
    var DataBlock = new Array[RDD[(Int, SumZ_Object)]](K); //K batch in memory
    //Not All Layer(DataBlock) is useful,It Record by Bellow two args
    var s_Start = 0; //Data Start Index
    var s_Length = 0; //Data Block size
    var l_Start = 0; //Layer Start Index

    var res = ""

    //2.Batch dealing data
    while (i < B + K - 1) { //total batch = B + K 
      //Test
      println("------------ Round:" + i + " ------------")
      //println("Round Start stamp:" + stamp)
      //2.1 Get this Round Information
      val tmp = GetLayerSize(i, K, B)
      s_Start = tmp._1
      s_Length = tmp._2
      l_Start = tmp._3
      //Test:
      //println("Start/Length/layerStart:" + s_Start + "/" + s_Length + "/" + l_Start)

      //2.2 Replace the DataBlock(X) with new input & persist it
      if (i < B) {
        DataBlock(s_Start) = sc.textFile(inputPath + i).map(StringToBatchObject_Dis)
      }
      //DataBlock(x).persist() //in memory
      //Test:DataBlock(x).saveAsTextFile(outputPath + "T" + i)

    
      //2.3 Add layer's keeper & Left to DataBlock And Join With PerG_f 
      //Note:G_f's b_id(i) and Data's b_id(j)
      //G_f's batch = i-j
      var j = s_Start //From start layer to End
      var l = l_Start //layer index
      var c = 0 //Size Counter
     
      while (c < s_Length) {
        //Test
        //println("--------" + "Layer" + l + "--------")
        //GET Keeper & Left(replaced Keeper)
        //1.Keeper
        DataBlock(j) = Union[(Int, (SumZ_Object, Double))](DataBlock(j),
          sc.parallelize(Seq((i - l, (sumZ.Get(l), Int.MaxValue: Double)))))
        //Test
        //println("keeper id:" + sumZ.Get(l).id)
        //2.Left
        DataBlock(j) = Union[(Int, (SumZ_Object, Double))](DataBlock(j),
          sc.parallelize(Seq((i - l, (sumZ.GetLeft(l), Int.MaxValue: Double)))))
        //println("Left id:" + sumZ.GetLeft(l).id)

        //Join With G_f .NOTE:Layer l's G_f for DataBlock(i - l)
        val PerG_f = sc.parallelize(Seq((i - l, sumZ.GetG_f(l))))
        DataBlock(j) = DataBlock(j).leftOuterJoin(PerG_f).map(MapToSumDis);
        //Test
        //println("G_f Min Distance:" + Gain.MinDisOfF(sumZ.GetG_f(l)))
        //println("DataBlock size:" + DataBlock(j).count())
        j = (j - 1 + K) % K //Next DataBlock Block
        l += 1 //Next layer
        c += 1
      }
      
      //2.4 Merge Per Batch Data
      //e.g. (b_id,Objects):(1,(O1,O2))(2,(O3,O4))
      var AllData = DataBlock(s_Start)

      j = (s_Start - 1 + K) % K //Not All Data merge
      c = 1
      while (c < s_Length) {
        AllData = AllData ++ DataBlock(j)
        //Test
        // val t1 = DataBlock(j).take(20)(19)
        //println("DataBlock(" + j + ") take(1) Info(Batch/object id):" + t1._1 + "/" + t1._2._1.id)
        //Next DataBlock

        j = (j - 1 + K) % K
        c += 1
      }
      //Test:
      //val a0 = AllData.take(1)(0);
      //println("AllData/a0:" + AllData.count() + "/" + a0._2._1.ShowAtt())
      

      //2.3 Add layer's keeper & Left to DataBlock And Join With PerG_f 
      var j = s_Start //From start layer to End
      var l = l_Start //layer index
      var c = 0 //Size Counter
      var OtherData = Seq[(Int, (SumZ_Object, Double))]()
      var G_f = Seq[(Int, SumZ_AttList)]()
      while (c < s_Length) {
        //1.Keeper
        OtherData = OtherData ++ Seq((i - l, (sumZ.Get(l), Int.MaxValue: Double)))
        //2.Left
        OtherData = OtherData ++ Seq((i - l, (sumZ.GetLeft(l), Int.MaxValue: Double)))
        //3.G_f
        G_f = G_f ++ Seq((i - l, sumZ.GetG_f(l)))
        l += 1 //Next layer
        c += 1
      }

      //2.4 Merge Per Batch Data
      var AllData = DataBlock(s_Start)
      j = (s_Start - 1 + K) % K //Not All Data merge
      c = 1
      while (c < s_Length) {
        AllData = AllData ++ DataBlock(j)
        //Test
        // val t1 = DataBlock(j).take(20)(19)
        //println("DataBlock(" + j + ") take(1) Info(Batch/object id):" + t1._1 + "/" + t1._2._1.id)
        //Next DataBlock

        j = (j - 1 + K) % K
        c += 1
      }
      AllData = (AllData ++ sc.parallelize(OtherData)).leftOuterJoin(sc.parallelize(G_f)).map(MapToSumDis);

      //2.5 Parallel Dealing
      val keeper = AllData.reduceByKey(NearestInBatch)
      //println("SumZ Start stamp:" + stamp)

      //2.6 Update keeper to sumZ
      val ns = SumZDescDis(keeper.take(s_Length),null,null)
      //Test
      //println("new sumZ size:" + ns.size)

      //2.7update sumZ and G_f
      sumZ.UpdateSumZ(ns, Math.max(0, i - B + 1))

      //2.8 next batch
      i += 1
      //println("Round End stamp:" + stamp)
      //3.Output Result

      res = "HierarchyStream algorithm SumZ Distance Lost: " + Gain.MinDisOfF((sumZ.GetG_f(Math.min(i, K)))) + "\nthe SumZ is : " + sumZ.s_ids + "\n"
      System.out.print(res)

    }
    //println("SumZ End stamp:" + stamp)
    //3.Save Result
    sc.parallelize(Seq(res)).saveAsTextFile(outputPath + "HS" + SaveStamp)

    //4. Ends
    println("Done!")
    sc.stop()
    */
  }
}