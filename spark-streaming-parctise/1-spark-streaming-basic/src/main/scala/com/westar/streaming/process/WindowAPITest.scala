package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WindowAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WindowAPITest")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc,Seconds(2))

    val lines = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_AND_DISK)
    //每过1秒钟，然后显示前3秒的数据
    val windowDStream = lines.window(Seconds(20), Seconds(2))
    windowDStream.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()

    ssc.stop(false)
  }

}
