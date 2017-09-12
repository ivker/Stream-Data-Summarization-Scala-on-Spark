package DataProcessing

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._

import Common._

object GenData {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataGeneration--Guassion")
    val sc = new SparkContext(conf)

    // global setting
    val dimension = args(0).toInt; // total dimension
    val no = args(1).toInt // number of objects of per batch
    val b = args(2).toInt //batch size

    // Guassion Distribution setting
    val v = args(3).toDouble // variance

    //Guassion Setting
    val P1 = args(4).toDouble
    val P2 = args(5).toDouble
    val P3 = args(6).toDouble

    //save setting
    val path = args(7)

    GuassionDistribution(sc, dimension, no, b, v, path, P1, P2, P3);

    sc.stop()
  }

  /*
   * Data satisfy Guassion Distribuion
   */

  def GuassionDistribution(sc: SparkContext, d: Int, no: Int, b: Int, v: Double, path: String,
                           P1: Double, P2: Double, P3: Double) {

    // initial setting
    var attcount = new Array[Int](d) //attribute count

    var attPro = new Array[Double](d) // the probability of attribute
    // 1. generate attribute probility
    var i = 0
    while (i < d) {
      attPro(i) = RandomGaussian01(1)
      i += 1 //Next
    }

    // 2.generate no objects & b batches
    var seq = Seq[String](); // All Data
    var o = ""; //Per Data
    var obPro = RandomGaussian01(1) // the probability of object generate 1
    var all0 = true
    // the probability of object is same
    i = 0 //batch
    var j = 0 //no per batch
    var k = 0 //attribute per object
    while (i < b) {
      j = 0
      seq = Seq[String](); //New Batch
      while (j < no) {
        //Main Data
        //New Data
        o = (i * no + j) + " "; //Object id
        obPro = RandomGaussian01(1)
        all0 = true //make sure there is not none attribute object
        //Random attributes
        k = 0
        var nomore = 0; //no more than d/10 attribute
        while (k < d) {
          val r = Math.round(P1 * obPro + P2 * attPro(k) + P3
            * Math.random()); //= 1 or 0
          if (r != 0) {
            o += k + " " //attribute id
            all0 = false; // a object at least include a attribute.
            nomore += 1
          }
          if (nomore > d / 10)
            k = d //break
          k += 1
        }

        // this object is full of 0
        if (!all0) {
          seq = seq :+ o
          //Test
          println("Object " + (i * no + j) + "done")
        } else {
          j -= 1 //Again
        }
        //End
        j += 1
      }
      //3. Per batch end save result
      sc.parallelize(seq).saveAsTextFile(path + i)
      i += 1
    }
    //end process

  }

  /*
   * Random a number satisfy 0-1 Gaussian distribution
   * 
   * @param v:variance 0.1 is not bad
   */
  def RandomGaussian01(v: Double): Double = {
    var g = Math
      .abs(Math.round(RandomGaussian(0, v) * 10000) / 10000.0);
    while (g > 1) {
      g = Math.abs(Math.round(RandomGaussian(0, v) * 10) / 10.0);
    }
    return g;
  }

  /*
   * Random a number satisfy Gaussian distribution
   * 
   * @param e:mathematical expectation
   * 
   * @param v:variance
   */
  def RandomGaussian(e: Double, v: Double): Double = {
    val r = new java.util.Random();
    return Math.sqrt(v) * r.nextGaussian() + e;
  }
}