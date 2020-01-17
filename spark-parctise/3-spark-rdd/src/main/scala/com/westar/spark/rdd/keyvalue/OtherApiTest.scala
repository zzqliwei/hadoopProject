package com.westar.spark.rdd.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * distinct
 *mapValues
 *
 * filterByRange
 */
object OtherApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(2, 3, 3, 6, 2))
    rdd.distinct().collect()

    val pairRDD =
      sc.parallelize[(Int, Int)](Seq((5, 2), (7, 4), (3, 3), (2, 4)), 4).partitionBy(new HashPartitioner(2))

    val mapValuesRDD = pairRDD.mapValues(x => x + 1)
    mapValuesRDD.collect()

    mapValuesRDD.partitioner //会记住父亲RDD的分区器

    val flatMapValuesRDD = pairRDD.flatMapValues(x => (x to 5))
    flatMapValuesRDD.collect()

    flatMapValuesRDD.partitioner

    pairRDD.keys.collect()

    pairRDD.values.collect()

    pairRDD.sortByKey().collect()

    pairRDD.sortByKey(false).collect()

    pairRDD.sortBy(_._1).collect()
    pairRDD.sortBy(_._1, false).collect()

    val rangeTestRDD =
      sc.parallelize[(Int, Int)](Seq((5, 2), (7, 4), (3, 6), (2, 6), (3, 6), (2, 6)), 4)
    rangeTestRDD.filterByRange(3, 5).collect()
  }

}
