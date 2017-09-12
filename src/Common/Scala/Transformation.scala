package Common.Scala

import org.apache.spark.rdd._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Transformation {
  def Union[T](r1: RDD[T], r2: RDD[T]): RDD[T] = {
    if (r2 != null)
      return r1.union(r2)
    else
      return r1
  }

  def UnionAll[T](r: Array[RDD[T]]): RDD[T] = {
    var res = r(0)
    var i = 1
    while (i < r.length) {
      res = Union(res, r(i))
      i += 1
    }
    return res
  }
}