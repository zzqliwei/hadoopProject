package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object JoinAPITest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("JoinAPITest")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val lines1 = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_AND_DISK)

    val kvs1 = lines1.map{ line =>
      val arr = line.split(" ")
      (arr(0),arr(1))
    }

    val lines2 = ssc.socketTextStream("master", 9997, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs2 = lines2.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }
    kvs1.join(kvs2).print()
    kvs1.fullOuterJoin(kvs2).print()
    kvs1.leftOuterJoin(kvs2).print()
    kvs1.rightOuterJoin(kvs2).print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)

    //等待Streaming程序终止
    ssc.awaitTermination()

  }
}
