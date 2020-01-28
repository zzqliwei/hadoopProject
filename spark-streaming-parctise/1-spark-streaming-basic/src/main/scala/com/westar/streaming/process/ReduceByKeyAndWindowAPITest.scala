package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceByKeyAndWindow
 */
object ReduceByKeyAndWindowAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ReduceByKeyAndWindowAPITest")
    val sc = new SparkContext(sparkConf)
    val ssc= new StreamingContext(sc,Seconds(2))

    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")

    val lines = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_AND_DISK)

    val words = lines.flatMap(_.split(" "))

    //每5秒中，统计前20秒内每个单词出现的次数
    val wordPair = words.map(x => (x, 1))

    val wordCounts =
      wordPair.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(5))

    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)


  }

}
