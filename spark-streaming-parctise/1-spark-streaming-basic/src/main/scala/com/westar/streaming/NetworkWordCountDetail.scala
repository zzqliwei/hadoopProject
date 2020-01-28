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
  *spark-submit --class com.twq.streaming.NetworkWordCountDetail \
  *--master spark://master:7077 \
  *--deploy-mode client \
  *--driver-memory 512m \
  *--executor-memory 512m \
  *--total-executor-cores 4 \
  *--executor-cores 2 \
  * /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar
*/
object NetworkWordCountDetail {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountDetail")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    //1、StreamingContext 是 Spark Streaming程序的入口，那么StreamingContext和SparkContext的关系是什么呢？
    //1.1、StreamingContext需要持有一个SparkContext的引用
    val ssc = new StreamingContext(sc,Seconds(2))

    //1.2、如果SparkContext没有启动的话，我们可以用下面的代码启动一个StreamingContext
    val ssc2 = new StreamingContext(sparkConf,Seconds(2))
    ssc.sparkContext //可以从StreamingContext中获取到SparkContext
    //1.3、对StreamingContext调用stop的话，可能会将SparkContext stop掉，
    // 如果不想stop掉SparkContext，我们可以调用
    ssc.stop(false)

    sc.stop()


    //2：StreamingContext的注意事项：
    // 2.1、在同一个时间内，同一个JVM中StreamingContext只能有一个
    // 2.2、如果一个StreamingContext启动起来了，
    //    那么我们就不能为这个StreamingContext添加任何的新的Streaming计算
    // 2.3、如果一个StreamingContext被stop了，那么它不能再次被start
    // 2.4、一个SparkContext可以启动多个StreamingContext，
    //    前提是前面的StreamingContext被stop掉了，而SparkContext没有被stop掉

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" " ))
    val wordCount = words.map(x =>(x,1)).reduceByKey(_ + _)

    //将结果输出到控制台
    wordCount.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()

  }

}
