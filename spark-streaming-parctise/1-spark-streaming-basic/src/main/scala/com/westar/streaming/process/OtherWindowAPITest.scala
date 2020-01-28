package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * countByWindow
 *
 * countByValueAndWindow
 */
object OtherWindowAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OtherWindowAPITest")
    val sc = new SparkContext(sparkConf)
    val ssc= new StreamingContext(sc,Seconds(2))

    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    //每过2秒钟，然后显示统计前10秒的数据量
    lines.countByWindow(Seconds(10), Seconds(2)).print()

    val words = lines.flatMap(_.split(" "))

    //每过2秒钟，然后用reduce func来聚合前10秒的数据
    words.reduceByWindow((a: String, b: String) => a + b, Seconds(10), Seconds(2)).print()

    //每过2秒钟，对前10秒的单词计数，相当于words.map((_, 1L)).reduceByKeyAndWindow(_ + _, _ - _)
    words.countByValueAndWindow(Seconds(10), Seconds(2)).print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)

    //等待Streaming程序终止
    ssc.awaitTermination()

  }

}
