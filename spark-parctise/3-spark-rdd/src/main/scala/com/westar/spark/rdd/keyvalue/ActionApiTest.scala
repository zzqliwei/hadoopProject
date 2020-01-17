package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 元组操作
 * collectAsMap
 * lookup
 */
object ActionApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ActionApiTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6)), 2)
    //将二元转换成map
    pairRDD.collectAsMap()
    //查询二元组
    pairRDD.lookup(3)

  }

}
