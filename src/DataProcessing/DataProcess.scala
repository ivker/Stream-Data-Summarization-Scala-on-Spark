package DataProcessing

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._

object DataProcess {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: DataProcess <in> <out>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("DataProcess")
    val sc = new SparkContext(conf)

    //main process
    val org = sc.textFile(args(0))
    org.map(DealString(_)).saveAsTextFile(args(1))
    //end process

    sc.stop()
  }

  /*
 * input:4 |user 1 9 11 13 23 16 18 17 19 15 43 14 39 30 66 50 27 104 20 |id ..|id
 * return: 1 9 11 13 23 16 18 17 19 15 43 14 39 30 66 50 27 104 20
 */
  def DealString(in: String): String = {
    in.substring(0, in.indexOf(" |i")).split("r ")(1)

  }

}
