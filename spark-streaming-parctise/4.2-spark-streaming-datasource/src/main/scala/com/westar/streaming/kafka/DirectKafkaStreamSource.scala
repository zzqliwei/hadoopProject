package com.westar.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  *1、创建topic1和topic2
  *bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic1
  *bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic2
  *2、查看topic
  *bin/kafka-topics.sh --describe --zookeeper master:2181 --topic topic1
 **
 3、启动Spark Streaming程序
   *spark-submit --class com.twq.streaming.kafka.DirectKafkaStreamSource \
   *--master spark://master:7077 \
   *--deploy-mode client \
   *--driver-memory 512m \
   *--executor-memory 512m \
   *--total-executor-cores 4 \
   *--executor-cores 2 \
   * /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
   * master:9092,slave1:9092,slave2:9092 topic1,topic2
 **
 4、模拟发送消息：bin/kafka-console-producer.sh --broker-list master:9092 --topic topic1
  */
object DirectKafkaStreamSource {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
