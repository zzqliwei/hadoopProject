package com.westar.submit

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object LocalSparkTest {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("LocalSparkTest")

    var sc = new SparkContext(conf)

    val sourceDataRDD = sc.textFile("")

    val wordsRDD = sourceDataRDD.flatMap(line => line.split(" "))

    val keyValueWordsRDD = wordsRDD.map(word => (word,1))

    val wordCountRDD = keyValueWordsRDD.reduceByKey(_ + _ )

    val outputFile = new File("/Users/tangweiqun/wordcount")
    
    if (outputFile.exists()) {
      val listFile = outputFile.listFiles()
      listFile.foreach(_.delete())
      outputFile.delete()
    }

    wordCountRDD.saveAsTextFile("file:///Users/tangweiqun/wordcount")
    sc.stop()
  }

}
