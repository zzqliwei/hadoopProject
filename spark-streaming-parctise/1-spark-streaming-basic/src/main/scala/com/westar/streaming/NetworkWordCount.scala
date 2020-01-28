package com.westar.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
 *
 * 1、在master服务器上启动一个Netcat server
 * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
 *
 * 2、用下面的命令在在集群中将Spark Streaming应用跑起来
   *spark-submit --class com.twq.streaming.NetworkWordCount \
   *--master spark://master:7077 \
   *--deploy-mode client \
   *--driver-memory 512m \
   *--executor-memory 512m \
   *--total-executor-cores 4 \
   *--executor-cores 2 \
   * /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar
 */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)

    // StreamingContext 编程入口
    val ssc = new StreamingContext(sc,Seconds(2))

    //数据接收器(Receiver)
    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_AND_DISK)

    val word = lines.flatMap( _.split(" "))
    val wordPair = word.map((_,1))

    val wordCounts = wordPair.reduceByKey(_ + _)
    //结果输出(Output)
    //将结果输出到控制台
    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()
  }
}
