package ScalaStudy

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
object Show {
   def main(args: Array[String]) {
     
    val conf =new SparkConf().setAppName("Show")
    val sc = new SparkContext(conf)
    
    val kv1 =sc.parallelize(Array(("A",3),("B",5),("A",6)))
   
    
    sc.stop()
  }
}