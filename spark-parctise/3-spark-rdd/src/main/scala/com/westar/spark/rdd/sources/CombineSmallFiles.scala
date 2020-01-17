package com.westar.spark.rdd.sources

import org.apache.spark.{SparkConf, SparkContext}

object CombineSmallFiles {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("BinaryDataFileFormat")
    conf.setMaster("local[*]")
    val sc = new SparkContext()

    val wholeTextFiles = sc.wholeTextFiles("hdfs://hadoop0:9000/users/hadoop/text/")

    wholeTextFiles.collect()
    wholeTextFiles.coalesce(1).saveAsSequenceFile("hdfs://hadoop0:9000/users/hadoop/seq/")
  }
}
