package com.westar.spark.rdd

import java.io.File

import com.twitter.chill.Output
import org.apache.spark.SparkContext


/**
 * val sc = new SparkContext("local[*]","LocalSparkTest")
 */
object LocalSparkTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","LocalSparkTest")

    val sourceDataRDD = sc.textFile("file:///Users/tangweiqun/test.txt")

    val wordsRDD = sourceDataRDD.flatMap(x => x.split(" "))

    val keyValueWordsRDD = wordsRDD.map(word => (word,1))

    val wordCountRDD = keyValueWordsRDD.reduceByKey( _ + _)

    val outputFile = new File("/Users/tangweiqun/wordcount")
    if(outputFile.exists()){
      val listFiles = outputFile.listFiles()
      listFiles.foreach(file => file.deleteOnExit())
      outputFile.delete()
    }

    wordCountRDD.saveAsTextFile("file:///Users/tangweiqun/wordcount")

    wordCountRDD.collect().foreach(println(_))
  }

}
