package DataProcessing

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._

object ydata {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: DataProcess <dimension> <repartition> <in> <out>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("DataProcess-ydata")
    val sc = new SparkContext(conf)

    //1. initial algorithm
    val dim = args(0).toInt
    val rep = args(1).toInt
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
        if (x.indexOf(" |i") == (-1) || x.substring(0, x.indexOf(" |i")).split("r ").size == 1) {
          ""
        } else {
          val tmp = x.substring(0, x.indexOf(" |i")).split("r ")(1).split(" ")

          val itor = tmp.iterator
          val data = new Array[Int](tmp.size)
          var index = 0 //useful data / stop index 
          var res = ""

          // 1.load data ignore value > dim
          while (itor.hasNext) {
            val t = itor.next.toInt
            if (t <= dim) {
              data(index) = t
              index += 1
            }
          }

          // 2.sort
          //Sort:Find Max value in each round -- desc
          var i = 0
          var t = 0
          index -= 1 //stop index
          while (i < index) {
            var j = index
            while (j > i) {
              //Sort By batch id
              if (data(j) > data(j - 1)) { //swap
                t = data(j)
                data(j) = data(j - 1)
                data(j - 1) = t
              }
              j -= 1
            }
            i += 1
          }

          // 3.combine
          i = 0
          while (i < index) {
            var j = i + 1
            while (j < index) {
              res = res + data(i).toString + data(j) + "," //unprocess conflict
              j += 1
            }
            i += 1
          }

          //4.return
          res

        }
      }
    }
    AllData.persist()

    // 3.split
    val splitData = AllData.repartition(rep)

    val saveData = splitData.mapPartitionsWithIndex((id, iter) => {
      for (i <- iter) yield id + ",-9999," + i
    })

    // 4.save
    saveData.saveAsTextFile(save)
    //    val test = sc.parallelize(for (i <- 0 until 100000) yield i, 10)
    //    test.saveAsTextFile("/hello")
    //    test.mapPartitions(iter => {
    //      List(iter.size).iterator
    //    }).collect.foreach(println(_))
    // 5.end
    sc.stop()
  }

}