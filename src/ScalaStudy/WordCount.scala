package ScalaStudy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
   def main(args: Array[String]) {
    if (args.length !=2 ){
      println("args: <input> <ouput>")
      return
    }
    val conf =new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))
    val result = textFile.flatMap(line => line.split(" "))
        .map(word => (word, 1)).reduceByKey(_ + _)
    result.saveAsTextFile(args(1))
  
    sc.stop()
  }
}