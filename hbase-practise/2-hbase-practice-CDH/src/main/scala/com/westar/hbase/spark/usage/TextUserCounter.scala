package com.westar.hbase.spark.usage

import org.apache.spark.{SparkConf, SparkContext}

object TextUserCounter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TextUserCounter")
    sparkConf.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val textFile = sparkContext.textFile("hdfs://master:9999/user/hadoop-twq/hbase-course/spark/data.txt")
    val userCountRDD = textFile.map(line => (line.substring(0, line.indexOf("|")), 1))
      .reduceByKey( _ + _ )

    println(s"we have generated : ${userCountRDD.count()} users" )
  }

}
