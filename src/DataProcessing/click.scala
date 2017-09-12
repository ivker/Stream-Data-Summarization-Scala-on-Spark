package DataProcessing

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._

object click {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: DataProcess <start Bid> <batch size> <in> <out>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("DataProcess-ydata")
    val sc = new SparkContext(conf)

    //1. initial algorithm
    var start = args(0).toInt //data start id
    val bs = args(1).toInt //batch size
    val load = args(2)
    val save = args(3)

    //2. processing Data To Our Format
    val org = sc.textFile(load)
    val AllData = org.map { x =>
      {
        /*
        * input:4 |user 1 9 11 13 23 16 18 17 19 15 43 14 39 30 66 50 27 104 20 |id ..|id
        * return: 1 9 11 13 23 16 18 17 19 15 43 14 39 30 66 50 27 104 20
        */
        val tmp = x.substring(0, x.indexOf(" |i")).split("r ")(1).split(" ")
        val itor = tmp.iterator

        // 1.load data
        var res = ""
        while (itor.hasNext) {
          res = res + itor.next + ","
        }

        //2.return
        res
      }
    }

    //3. save as batch
    val size = AllData.count()
    val Data = AllData.collect()

    var i = 0 //all Data Index
    var bc = 0; //batch size counter
    var batch = Seq[String]() // batch data
    while (i < size) {
      if ((bc == bs) || (i == size - 1)) {
        //save this batch
        val DD = sc.parallelize(batch)
        DD.saveAsTextFile(save + start)
        start += 1 //next save batch id

        //Reset args
        bc = 0
        batch = Seq[String]()
      }

      val ne = i + "," + Data(i) //new element with id
      batch = batch :+ (start + "," + ne) //Add 
      i += 1 //Next Data
      bc += 1
      if (i % 1000 == 0)
        println("Dealing:" + i + "")
    }

    sc.stop()
  }

}