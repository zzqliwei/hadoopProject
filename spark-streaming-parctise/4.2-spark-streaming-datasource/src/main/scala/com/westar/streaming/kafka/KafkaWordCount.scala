package com.westar.streaming.kafka
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  1、创建topic1和topic2
  bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic1
  bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic2
  2、查看topic
  bin/kafka-topics.sh --describe --zookeeper master:2181 --topic topic1

  3、启动Spark Streaming程序
   spark-submit --class com.twq.streaming.kafka.KafkaWordCount \
   --master spark://master:7077 \
   --deploy-mode client \
   --driver-memory 512m \
   --executor-memory 512m \
   --total-executor-cores 4 \
   --executor-cores 2 \
   /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
   master,slave1,slave2 my-consumer-group topic1,topic2 1

   4、模拟发送消息：bin/kafka-console-producer.sh --broker-list master:9092 --topic topic1
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)


    val Array(zkQuorum, group, topics, numThreads) = args

    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("hdfs://master:9999/user/spark-course/streaming/checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "largest")//smallest

    //有多个的时候
    val numStreams = 3
    val kafkaStreams = (1 to numStreams).map{_ =>
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    }
    val unifiedStream = ssc.union(kafkaStreams)
    //只有一个的时候
    val kafkaDStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)

    val lines = unifiedStream.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)



  }

}
