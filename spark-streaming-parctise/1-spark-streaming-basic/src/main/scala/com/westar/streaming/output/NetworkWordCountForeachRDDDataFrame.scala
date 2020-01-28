package com.westar.streaming.output

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
 *
 * 1、在master服务器上启动一个Netcat server
 * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
 *
 *
 */
object NetworkWordCountForeachRDDDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(1))

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))

    //将RDD转化为Dataset
    words.foreachRDD{ rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val wordsDataFrame = rdd.toDF("word")
      wordsDataFrame.createGlobalTempView("words")

      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    }
    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)

    //将RDD转化为Dataset
    words.foreachRDD { (rdd, time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame = wordsDataFrame.groupBy("word").count()

      val resultDFWithTs = wordCountsDataFrame.rdd.map(row => (row(0), row(1), time.milliseconds)).toDF("word", "count", "ts")

      resultDFWithTs.write.mode(SaveMode.Append).parquet("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/parquet")
    }

    //等待Streaming程序终止
    ssc.awaitTermination()


  }
}
