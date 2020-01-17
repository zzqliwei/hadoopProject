package com.westar.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * Produces a count of events received from Flume.
  *
  *  This should be used in conjunction with the Spark Sink running in a Flume agent. See
  *  the Spark Streaming programming guide for more details.
  *
  * Pull-based Approach using a Custom Sink(Spark Streaming作为一个Sink存在)
  *
  * 1、将jar包scala-library_2.11.8.jar(这里一定要注意flume的classpath下是否还有其他版本的scala，要是有的话，则删掉，用这个，一般会有，因为flume依赖kafka，kafka依赖scala)、
  * commons-lang3-3.5.jar、spark-streaming-flume-sink_2.11-2.2.0.jar
  * 放置在master上的/home/hadoop-twq/spark-course/streaming/spark-streaming-flume/apache-flume-1.8.0-bin/lib下
  *
  * 2、配置/home/hadoop-twq/spark-course/streaming/spark-streaming-flume/apache-flume-1.8.0-bin/conf/flume-conf.properties
  *
  * 3、启动flume的agent
  * bin/flume-ng agent -n agent1 -c conf -f conf/flume-conf.properties
  *
  * 4、启动Spark Streaming应用
 *spark-submit --class com.twq.streaming.flume.FlumeEventCountPullBased \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 4 \
 *--executor-cores 2 \
 */home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *master 44446
 **
 3、在master上 telnet localhost 44445 发送消息
 *
 */
object FlumeEventCountPullBased {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventCount <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
