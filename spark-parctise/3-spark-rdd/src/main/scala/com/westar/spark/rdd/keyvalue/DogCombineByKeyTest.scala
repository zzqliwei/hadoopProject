package com.westar.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

object DogCombineByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DogCombineByKeyTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
  }

}
