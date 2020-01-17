package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * cogroup
 * groupWith
 * join
 * rightOuterJoin
 * leftOuterJoin
 * fullOuterJoin
 * subtractByKey
 */
object CogroupApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CogroupApiTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)

    val otherRDD = sc.parallelize(Seq((3, 9), (4, 5)))

    //res0: Array[(Int, (Iterable[Int], Iterable[Int]))]
    // = Array((4,(CompactBuffer(),CompactBuffer(5))), (1,(CompactBuffer(2),CompactBuffer())),
    // (5,(CompactBuffer(6),CompactBuffer())), (3,(CompactBuffer(6, 4),CompactBuffer(9))))
    pairRDD.cogroup(otherRDD).collect()

    //groupWith是cogroup的别名，效果和cogroup一摸一样
    pairRDD.groupWith(otherRDD).collect()

    // Array[(Int, (Int, Int))] = Array((3,(4,9)), (3,(6,9)))
    pairRDD.join(otherRDD).collect()

    // Array[(Int, (Option[Int], Int))] = Array((4,(None,5)), (3,(Some(4),9)), (3,(Some(6),9)))
    pairRDD.rightOuterJoin(otherRDD).collect()

    pairRDD.leftOuterJoin(otherRDD).collect()

    // Array[(Int, (Option[Int], Option[Int]))]
    // = Array((4,(None,Some(5))), (1,(Some(2),None)), (5,(Some(6),None)),
    // (3,(Some(4),Some(9))), (3,(Some(6),Some(9))))
    pairRDD.fullOuterJoin(otherRDD).collect()

    // 减掉相同的key, 这个示例减掉了为3的key
    // Array[(Int, Int)] = Array((1,2), (5,6))
    pairRDD.subtractByKey(otherRDD).collect()


  }

}
