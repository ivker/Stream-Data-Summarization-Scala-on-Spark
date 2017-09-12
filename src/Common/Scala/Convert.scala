package Common.Scala

import Common._
import org.apache.spark.rdd._

import java.util.ArrayList;
import java.util.Date;

object Convert {

  /**
   * input format:1 32 42 55 62 14
   * first number is id,others are data
   */
  def StringToBatchObject_Cov(data: String): (Int, Common.SumZ_Object) = {

    var value = data.split(",")
    var itor = value.iterator
    var batchid = itor.next().toInt
    var o = new Common.SumZ_Object(itor.next().toInt) //initial with id
    while (itor.hasNext) {
      o.Add(new Common.SumZ_Att(itor.next().toInt, 1)) //add data
    }
    return (batchid, o)
  }

  /**
   * input format:1,0,9,2,4,2,5,2,3
   * first number is batch id,second is id,others are distance to other object
   */
  def StringToBatchObject_Dis(data: String): (Int, Common.SumZ_Object) = {
    var i = 0
    var value = data.split(",")
    var itor = value.iterator
    var batchid = itor.next().toInt
    var o = new Common.SumZ_Object(itor.next().toInt) //initial with id
    while (itor.hasNext) {
      o.Add(new Common.SumZ_Att(i, itor.next.toInt)) //add data
      i += 1
    }
    return (batchid, o)
  }

  /**
   * Map Data To each Batch:Per Object to many batch
   * b:Batch size
   */
  def ObjectWithBatch(o: SumZ_Object, b: Int): Array[(Int, SumZ_Object)] = {
    var res = new Array[(Int, SumZ_Object)](b)
    var i = 0
    while (i < b) {
      res(i) = (i, o)
      i += 1
    }
    return res
  }

  /**
   * Choose the best result in this layer & same batch(Key)
   * RDD[(int,SumZ_Object,SumZ_AttList)] means
   * batch_id,data,G_f
   */
  def BestInBatch(v1: (SumZ_Object, Double),
                  v2: (SumZ_Object, Double)): (SumZ_Object, Double) = {
    if (v1._2 > v2._2)
      //return the large gain or the older keeper one(smaller id)
      return v1
    else if (v1._2 == v2._2 && v1._1.id < v2._1.id)
      return v1
    else
      return v2;
  }

  /**
   * Choose the best result in this layer & same batch(Key)
   * RDD[(int,SumZ_Object,SumZ_AttList)] means
   * batch_id,data,G_f
   */
  def NearestInBatch(v1: (SumZ_Object, Double),
                     v2: (SumZ_Object, Double)): (SumZ_Object, Double) = {
    if (v1._2 < v2._2)
      //return the lower sum distance or the older keeper one(smaller id)
      return v1
    else if (v1._2 == v2._2 && v1._1.id < v2._1.id)
      return v1
    else
      return v2;
  }

  /**
   * Deal Per Object
   *
   * return:SumZ_Object.id =
   * -1 means Only sumZ_Siever useful
   * other means undealed
   */
  def SieveDataCov(v1: (SumZ_Object, SumZ_SieverCov),
                   v2: (SumZ_Object, SumZ_SieverCov)): (SumZ_Object, SumZ_SieverCov) = {
    //Data 1
    var no = v1._1 //new object
    var ns = v1._2 //new sumZ_Siever
    if (no.id != (-1)) //this one had not been processed
    {
      //1.>=target gain
      if ((ns.s.Size() < ns.K) && Gain.GOOoutF(no, ns.s.C_f) >= ns.TargetGain) {
        ns.AddWithUpdate(no)
      } else {
        //2.get new sumZ_Siever
        ns = v2._2
      }
    } else if (no.id == (-1)) { //Only sumZ_Siever useful
      //keep v1's ns
    } else { //ns = v2._2.get
      ns = v2._2
    }

    // Next Object
    no = v2._1
    //this one had not been processed
    if ((ns.s.Size() < ns.K) && no.id != (-1) && (Gain.GOOoutF(no, ns.s.C_f) >= ns.TargetGain)) {
      //>=target gain
      ns.AddWithUpdate(no)
    }

    //return no.id = -1
    return (new SumZ_Object(-1), ns);
  }

  /**
   * Deal Per Object
   *
   * return:SumZ_Object.id =
   * -1 means Only sumZ_Siever useful
   * other means undealed
   */
  def SieveDataDis(v1: (SumZ_Object, SumZ_SieverDis),
                   v2: (SumZ_Object, SumZ_SieverDis)): (SumZ_Object, SumZ_SieverDis) = {
    //Data 1
    var no = v1._1 //new object
    var ns = v1._2 //new sumZ_Siever
    if (no.id > (-1)) //this one had not been processed
    {
      //1.>=target gain
      if ((ns.s.Size() < ns.K) && (Gain.MinDisofSumZ(ns.s) - Gain.MinDisWithF(no, ns.s.D_f)) >= ns.TargetGain) {
        ns.AddWithUpdate(no)
      } else {
        //2.get new sumZ_Siever
        ns = v2._2
      }
    } else if (no.id == (-1)) { //Only sumZ_Siever useful
      //keep v1's ns
    } else { //ns = v2._2.get
      ns = v2._2
    }

    // Next Object
    no = v2._1
    //this one had not been processed
    if ((ns.s.Size() < ns.K) && no.id > (-1) && (Gain.MinDisofSumZ(ns.s) - Gain.MinDisWithF(no, ns.s.D_f)) >= ns.TargetGain) {
      //>=target gain
      ns.AddWithUpdate(no)
    }

    //return no.id = -1
    return (new SumZ_Object(-1), ns);
  }

  /**
   * Calculate all Data's gain with G_f,except input gain is 0
   */
  def MapToGain(data: (Int, ((SumZ_Object, Double), Option[SumZ_AttList]))): (Int, (SumZ_Object, Double)) = {
    if (data._2._1._2 != 0) {
      val gain = Gain.GOOoutF(data._2._1._1, data._2._2.get)
      return (data._1, (data._2._1._1, gain));
    } else
      return (data._1, (data._2._1));
  }

  /**
   * Calculate all Data's gain with G_f,except input gain is 0
   */
  def MapToSumDis(data: (Int, ((SumZ_Object, Double), Option[SumZ_AttList]))): (Int, (SumZ_Object, Double)) = {
    val gain = Gain.MinDisWithF(data._2._1._1, data._2._2.get)
    return (data._1, (data._2._1._1, gain));
  }

  /**
   * sumZ sort By key(Batch) DESC
   */
  def SumZDescDis(data: Array[(Int, (SumZ_Object, Double))], Keeper: Array[(SumZ_Object, Double)], Left: Array[(SumZ_Object, Double)]): ArrayList[SumZ_Object] = {
    var ns = new ArrayList[SumZ_Object]()
    var tmp = (0, (null, 0)): (Int, (SumZ_Object, Double))
    var i = 0
    var end = data.size - 1; //stop index
    //Sort:Find Max id in each round
    while (i < end) {
      var j = end
      while (j > i) {
        //Sort By batch id
        if (data(j)._1 > data(j - 1)._1) { //swap
          tmp = data(j)
          data(j) = data(j - 1)
          data(j - 1) = tmp
        }
        j -= 1
      }
      //keeper / left and new object which one is the best
      val go = data(i)._2._2
      val gk = Keeper(i)._2
      val gl = Left(i)._2
      if (go < gk && go < gl) {
        ns.add(data(i)._2._1)
      } else if (gl < gk) {
        ns.add(Left(i)._1)
      } else {
        ns.add(Keeper(i)._1)
      }

      i += 1
    }
    ns.add(data(end)._2._1) //Data(end) No Sort in new sumz

    return ns
  }

  /**
   * sumZ sort By key(Batch) DESC
   */
  def SumZDescCov(data: Array[(Int, (SumZ_Object, Double))], Keeper: Array[(SumZ_Object, Double)], Left: Array[(SumZ_Object, Double)]): ArrayList[SumZ_Object] = {
    var ns = new ArrayList[SumZ_Object]()
    var tmp = (0, (null, 0)): (Int, (SumZ_Object, Double))
    var i = 0
    var end = data.size - 1; //stop index
    //Sort:Find Max id in each round
    while (i < end) {
      var j = end
      while (j > i) {
        //Sort By batch id
        if (data(j)._1 > data(j - 1)._1) { //swap
          tmp = data(j)
          data(j) = data(j - 1)
          data(j - 1) = tmp
        }
        j -= 1
      }
      //keeper / left and new object which one is the best
      val go = data(i)._2._2
      val gk = Keeper(i)._2
      val gl = Left(i)._2
      if (go > gk && go > gl) {
        ns.add(data(i)._2._1)
      } else if (gl > gk) {
        ns.add(Left(i)._1)
      } else {
        ns.add(Keeper(i)._1)
      }

      i += 1
    }
    ns.add(data(end)._2._1) //Data(end) No Sort in new sumz

    return ns
  }

  def ToSieverListCov(data: Array[(Int, (SumZ_Object, SumZ_SieverCov))]): ArrayList[SumZ_SieverCov] = {
    var ns = new ArrayList[SumZ_SieverCov]()
    var i = 0
    while (i < data.size) {
      ns.add(data(i)._2._2)
      i += 1
    }
    return ns
  }

  def ToSieverListDis(data: Array[(Int, (SumZ_Object, SumZ_SieverDis))]): ArrayList[SumZ_SieverDis] = {
    var ns = new ArrayList[SumZ_SieverDis]()
    var i = 0
    while (i < data.size) {
      ns.add(data(i)._2._2)
      i += 1
    }
    return ns
  }

  /**
   * return the object of res by batch id
   */
  def ObjectOfGreeDi(batchId: Int, res: Array[(Int, (SumZ_Object, Double))]): SumZ_Object = {
    var i = 0
    while (i < res.length) {
      if (batchId == res(i)._1) {
        return res(i)._2._1
      } else
        i += 1
    }
    return null
  }

  def SaveStamp(): String = {
    val d = new Date();
    return (d.getMonth() + 1) + "" + d.getDate() + "-" + d.getHours() + "" + d.getMinutes()
  }

  def rand2(): Int = {
    val r = new java.util.Random()
    r.nextInt(99)
  }
}