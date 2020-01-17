package com.westar.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * Produces a count of events received from Flume.
  *
  *  This should be used in conjunction with an AvroSink in Flume. It will start
  *  an Avro server on at the request host:port address and listen for requests.
  *  Your Flume AvroSink should be pointed to this address.
  *
  * Flume-style Push-based Approach(Spark Streaming作为一个agent存在)
  *
  * 1、在slave1(必须要有spark的worker进程在)上启动一个flume agent
  * bin/flume-ng agent -n agent1 -c conf -f conf/flume-conf.properties
  *
  * 2、启动Spark Streaming应用
 *spark-submit --class com.twq.streaming.flume.FlumeEventCountPushBased \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 4 \
 *--executor-cores 2 \
 */home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *172.26.232.97 44446
 **
 3、在slave1上 telnet slave1 44445 发送消息
  */
object FlumeEventCountPushBased {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    val stream: DStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)
    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
