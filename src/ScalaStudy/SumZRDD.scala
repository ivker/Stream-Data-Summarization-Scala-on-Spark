package ScalaStudy

import org.apache.spark.rdd._
import scala.reflect.{ classTag, ClassTag }
import org.apache.spark._

class SumZRDD[K, V](self: RDD[(K, V)])
 (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends PairRDDFunctions[K, V](self) {

}