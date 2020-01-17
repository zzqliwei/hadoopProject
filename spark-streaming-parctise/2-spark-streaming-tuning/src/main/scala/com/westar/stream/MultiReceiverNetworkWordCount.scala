package com.westar.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
  *
  * 1、在master服务器上启动一个Netcat server
  * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
  *
  * 2、用下面的命令在在集群中将Spark Streaming应用跑起来
  * spark-submit --class com.twq.wordcount.JavaNetworkWordCount \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar
  *
  * spark-shell --master spark://master:7077 --total-executor-cores 4 --executor-cores 2
  */
object MultiReceiverNetworkWordCount {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("MultiReceiverNetworkWordCount")
    val sc = new SparkContext(sparkConf)

    var ssc = new StreamingContext(sc,Seconds(5))

    //创建多个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    var lines1 = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    var lines2 = ssc.socketTextStream("master", 9997, StorageLevel.MEMORY_AND_DISK_SER)

    val line = lines1.union(lines2)


    val words = line.repartition(100).flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey((a:Int,b:Int) =>(a + b),new HashPartitioner(10))

    //将结果输出到控制台
    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()

    ssc.stop(false)




  }

}
