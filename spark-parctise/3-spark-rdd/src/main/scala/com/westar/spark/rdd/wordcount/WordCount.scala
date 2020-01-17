package com.westar.spark.rdd.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")

    val sparkContext = new SparkContext(conf)

    val textFileRdd = sparkContext.textFile("hdfs://hadoop0:9000/user/hadoop/word.txt")

    val wordRDD = textFileRdd.flatMap(line => line.split(" "))

    val pairWordRDD = wordRDD.map(word => (word, 1))

    val wordCountRDD = pairWordRDD.reduceByKey( _ + _)

    wordCountRDD.saveAsTextFile("hdfs://hadoop0:9000/user/hadoop/wordcount")


  }

}
