package DataProcessing

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel

class st {
  var x = 0
  def getX = x
  def setX(t: Int) = { x = t }
}

object SendData {

  def updateFunc = (currValues: Seq[String], prevValueState: Option[st]) => {

    val s = prevValueState.getOrElse(new st);
    //val sh =currValues.toString()
    //println(sh)
    //println(s.x)
    // s.x += 1;
    Some(s)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SendData")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("hdfs://hadoop201:9000/zfx/cp/")

    val num = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)

    val kv = num.map { x => (1, x) }
    val update = kv.updateStateByKey[st](updateFunc)
    update.print()

    ssc.start()
    ssc.awaitTermination();
  }
}