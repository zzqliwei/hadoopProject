package com.westar.streaming

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
 *
 * 1、在master服务器上启动一个Netcat server
 * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
 */
object LocalNetworkWordCount {
  def main(args: Array[String]): Unit = {
    // StreamingContext 编程入口
    var ssc = new StreamingContext("local[2]", "LocalNetworkWordCount", Seconds(1),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass).toSeq)

    //数据接收器(Receiver)
    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("localhost",99998,StorageLevel.MEMORY_AND_DISK_SER)

    //数据处理(Process)
    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordPairs = words.map( x=>(x,1))

    val wordCounts = wordPairs.reduceByKey( _ + _)
    //结果输出(Output)
    //将结果输出到控制台
    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()
  }

}
