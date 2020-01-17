package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * emptyRDD
 * reduce
 * fold
 */
object ReduceFoldCompare {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    //1  rdd action api reduce and fold
    val emptyRdd = sc.emptyRDD[Int]
    emptyRdd.reduce(_ + _)
    //java.lang.UnsupportedOperationException: empty collection
    //  at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$apply$36.apply(RDD.scala:1027)

    emptyRdd.fold(0)(_ + _) // res1: Int = 0

    val testRdds = sc.parallelize(Seq(ArrayBuffer(0, 1, 3), ArrayBuffer(2, 4, 5)))
    // 会产生很多的中间临时对象 因为ArrayBuffer ++ ArrayBuffer会创建一个新的ArrayBuffer对象
    ArrayBuffer(0, 1, 3) ++ ArrayBuffer(0, 1, 3)
    testRdds.reduce(_ ++ _)
    // ArrayBuffer只初始化一次，每次都是将ArrayBuffer append到之前的ArrayBuffer中，不会产生中间临时对象
    ArrayBuffer(0, 1, 3) ++= ArrayBuffer(0, 1, 3)
    testRdds.fold(ArrayBuffer.empty[Int])((buff, elem) => buff ++= elem)

    //2 key-value rdd transformations api reduceByKey and foldByKey
    //空的RDD的行为是一样的
    val emptyKeyValueRdd = sc.emptyRDD[(Int, Int)]
    emptyKeyValueRdd.reduceByKey(_ + _).collect
    emptyKeyValueRdd.foldByKey((0))(_ + _).collect


    val testPairRdds = sc.parallelize(Seq(("key1", ArrayBuffer(0, 1, 3)),
      ("key2", ArrayBuffer(2, 4, 5)), ("key1", ArrayBuffer(2, 1, 3))))
    testPairRdds.reduceByKey(_ ++ _).collect()
    testPairRdds.foldByKey(ArrayBuffer.empty[Int])((buff, elem) => buff ++= elem).collect()

    //scala reduce and fold
    val seqEmpty = Seq.empty[Int]
    seqEmpty.reduce(_ + _)
    seqEmpty.fold(0)(_ + _)

    val seq = Seq(2, 3, 4)
    seq.reduce(_ + _)
    seq.fold(0)(_ + _)

    //只能返回seq元素类型及其子类型
    seq.reduceLeft(_ + _)

    //这个可以返回任意类型的数据
    seq.foldLeft(ArrayBuffer.empty[String])((buff, curr) => {
      if (curr == 2) {
        buff += curr.toString
      }
      buff
    })

  }

}
