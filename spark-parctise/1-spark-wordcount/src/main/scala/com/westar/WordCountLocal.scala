package com.westar

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountLocal")
    conf.setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val textFileRdd = sparkContext.textFile("spark-parctise/spark-wordcount/src/main/resources/word.txt")

    val wordRDD = textFileRdd.flatMap(line => line.split(" "))

    val pairWordRDD = wordRDD.map(word => (word, 1))

    val wordCountRDD = pairWordRDD.reduceByKey( _ + _)

    wordCountRDD.saveAsTextFile("spark-parctise/spark-wordcount/src/main/resources/wordcount")
  }

}
