package com.westar.streaming.receiver

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

object QueueStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("QueueStream")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(1))
    // 创建一个RDD类型的queue
    val rddQueue = new Queue[RDD[Int]]()

    // 创建QueueInputDStream 且接受数据和处理数据
    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey( _ + _)

    reducedStream.print()
    ssc.start()

    // 将RDD push到queue中，实时处理
    rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)

    ssc.stop(false)


  }

}
