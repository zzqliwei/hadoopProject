package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object UpdateStateByKeyAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UpdateStateByKeyAPITest")
    val sc= new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    val wordsDStream = words.map(x => (x, 1))

    wordsDStream.updateStateByKey((values: Seq[Int], currentState: Option[Int])
    => Some(currentState.getOrElse(0) + values.sum)).print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)

    //updateStateByKey的另一个API
    wordsDStream.updateStateByKey[Int]((iter: Iterator[(String, Seq[Int], Option[Int])]) => {
      val list = ListBuffer[(String, Int)]()
      while (iter.hasNext) {
        val (key, newCounts, currentState) = iter.next
        val state = Some(currentState.getOrElse(0) + newCounts.sum)

        val value = state.getOrElse(0)
        if (key.contains("error")) {
          list += ((key, value)) // Add only keys with contains error
        }
      }
      list.toIterator
    }, new HashPartitioner(4), true).print()

    ssc.start()


    //等待Streaming程序终止
    ssc.awaitTermination()

  }

}
