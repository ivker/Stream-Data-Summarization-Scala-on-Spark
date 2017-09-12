package ScalaStudy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._

object StreamCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("StreamWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.textFileStream("hdfs://hadoop201:9000/zfx/stream/")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination();
  }

}