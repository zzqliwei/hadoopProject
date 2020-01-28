package com.westar.streaming.pageview

import org.apache.avro.ipc.specific.PageView
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实时分析网站被访问的情况
 */
object PageViewStream {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: PageViewStream <metric> <host> <port>")
      System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
        " errorRatePerZipCode, activeUserCount, popularUsersSeen")
      System.exit(1)
    }

    val metric = args(0)
    val host = args(1)
    val port = args(2).toInt

    val ssc = new StreamingContext("local[2]", "PageViewStream", Seconds(1),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass).toSeq)
    ssc.checkpoint("/tmp/streaming")

    // 创建一个ReceiverInputDStream 用于接受socket host:port中的数据
    // 且将每一行的字符串转换成PageView对象
    val pageViews = ssc.socketTextStream(host,port)
      .flatMap(_.split("\n"))
      .map(PageView.fromString(_))

    // 1、统计每一秒中url被访问的次数
    val pageCounts = pageViews.map(view => view.url).countByValue()

    // 2、每隔2秒统计前10秒内每一个Url被访问的次数
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(10), Seconds(2))

    // 3、每隔2秒钟统计前30秒内每一个地区邮政编码的访问错误率情况(status非200的，表示是访问错误页面)
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(2))
      .map(view => (view.zipCode, view.status)).groupByKey()

    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip,statuses) =>
        val normalCount = statuses.count(_ == 200)
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
    }

    // 4、每隔2秒统计前15秒内有多少活跃用户
    val activeUserCount = pageViews.window(Seconds(15), Seconds(2))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("Unique active users: " + _)

    // 外部数据源(userId -> userName)，用于和流数据进行关联
    val userList = ssc.sparkContext.parallelize(Seq(
      1 -> "lao tang",
      2 -> "jeffy",
      3 -> "katy"))

    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
        // 5、统计每隔1秒内在已经存在的user是否为活跃用户
        pageViews.map(view => (view.userID, 1))
          .foreachRDD((rdd, time) => rdd.join(userList)
            .map(_._2._2)
            .take(10)
            .foreach(u => println("Saw user %s at time %s".format(u, time))))
      case _ => println("Invalid metric entered: " + metric)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
