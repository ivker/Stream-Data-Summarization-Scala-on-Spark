package ScalaStudy

import org.apache.spark.SparkConf

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel

class st {
  var x = 0
  def getX = x
  def setX(t: Int) = { x = t }
}

object MultiLayer {
  def updateFunc = (currValues: Seq[String], prevValueState: Option[st]) => {

    //val s = prevValueState.getOrElse(new st);
    //val sh =currValues.toString()
    //println(prevValueState)
    //println(s.x)
    // s.x += 1;
    Some(null)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MultiLayerCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val data = ssc.textFileStream("hdfs://hadoop201:9000/zfx/stream/")
    val lv = data.flatMap(_.split(" ")).map(x => (1, x))
    val ready = lv.updateStateByKey(updateFunc)
    ready.saveAsTextFiles("hdfs://hadoop201:9000/zfx/res/")

    ssc.start()
    ssc.awaitTermination();
  }

}