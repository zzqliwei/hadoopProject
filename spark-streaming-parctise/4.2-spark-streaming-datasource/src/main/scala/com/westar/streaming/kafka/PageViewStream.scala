package com.westar.streaming.kafka

import com.westar.streaming.InternalRedisClient
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实时分析网站被访问的情况 - 统计2秒钟内前30秒内有多少活跃用户
  * 1、启动redis
  *
  * 如果没有设置密码的话，则打开src/redis-cli。然后执行 CONFIG SET protected-mode no
  *
  * 2、配置flume
  *
  * 3、创建topic
  * bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic pageview
  *
  * bin/kafka-topics.sh --describe --zookeeper master:2181 --topic pageview
  *
  * 4、启动flume
  * bin/flume-ng agent -n agent1 -c conf -f conf/flume-conf.properties &
  *
  * 5、启动spark streaming
  * spark-submit --class com.twq.streaming.kafka.PageViewStream \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * /home / hadoop - twq / spark - course / streaming / spark - streaming - datasource - 1.0 - SNAPSHOT - jar - with - dependencies.jar \
  * master: 9092, slave1: 9092, slave2: 9092 pageview
  *
  *
  * 6 、 模拟打印pagevew的日志
  * echo http :// baidu.com 200 899787 22224 >> / home / hadoop - twq / spark - course / streaming / weblog.log
  */

object PageViewStream {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("PageViewStream")
    val sc = new SparkContext(sparkConf)
    val Array(brokers, topics) = args

    // Create the context
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")

    //从kafka中读取数据
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // 且将每一行的字符串转换成PageView对象
    val pageViews = messages.map { case (_, value) => PageView.fromString(value) }

    // 每隔2秒统计前15秒内有多少活跃用户
    val activeUserCount = pageViews.window(Seconds(30), Seconds(5))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()

    activeUserCount.foreachRDD{rdd =>
      rdd.foreachPartition{ partitionRecords =>
        val jedis = InternalRedisClient.getPool.getResource
        partitionRecords.foreach{ count =>
          jedis.set("active_user_count", count.toString)
        }
        InternalRedisClient.getPool.returnResource(jedis)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

/*
  表示访问一个网站页面的行为日志数据，格式为：
  url status zipCode userID\n
 */
case class PageView(val url: String, val status: Int, val zipCode: Int, val userID: Int)
  extends Serializable

object PageView extends Serializable {
  def fromString(in: String): PageView = {
    val parts = in.split(" ")
    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
  }
}
