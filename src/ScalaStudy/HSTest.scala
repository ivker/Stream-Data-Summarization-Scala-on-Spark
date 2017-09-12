package ScalaStudy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object HSTest {

  //Show ~~~ Test
  def show(b: (Int, String)) {
    println(b._2)
  }

  //Full Data Access
  def dealData(iterator: Iterator[(Int, String)]) {
    println("---New partition---")
    iterator.foreach(show)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("args: <input> <ouput>")
      return
    }
    val conf = new SparkConf().setAppName("HSTest")
    val sc = new SparkContext(conf)

    //1.Load Data
    val Data = sc.textFile(args(0), 3)

    //2.Map into K-V Format (9 3 4) => (1,9 3 4)
    val kvData = Data.map { x => (1, x) }
    kvData.persist() //in memory

    //3.Batch processing Data
    kvData.foreachPartition(dealData)

    sc.stop()
  }
}