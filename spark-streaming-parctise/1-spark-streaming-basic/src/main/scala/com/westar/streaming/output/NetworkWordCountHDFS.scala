package com.westar.streaming.output

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
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
 * spark-submit --class com.twq.streaming.output.NetworkWordCountHDFS \
 * --master spark://master:7077 \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --total-executor-cores 4 \
 * --executor-cores 2 \
 * /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar
 */
object NetworkWordCountHDFS {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountHDFS")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.repartition(1).mapPartitions{iter =>
      val text = new Text()
      iter.map{x=>
        text.set(x.toString())
        (NullWritable.get(), text)
      }
    } saveAsHadoopFiles[TextOutputFormat[NullWritable,Text]](
      "hdfs://master:9999/user/hadoop-twq/spark-course/streaming/data/hadoop/wordcount", "-hadoop")
  }
}
