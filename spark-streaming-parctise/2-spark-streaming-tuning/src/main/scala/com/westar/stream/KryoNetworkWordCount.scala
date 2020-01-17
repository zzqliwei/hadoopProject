package com.westar.stream

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
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
  * spark-submit --class com.twq.wordcount.JavaNetworkWordCount \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  * /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar
  */
object KryoNetworkWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KryoNetworkWordCount")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.twq.spark.rdd.example.ClickTrackerKryoRegistrator")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc,Seconds(5))

    //如果一个batchInterval中的数据量不大，并且没有window等操作，则可以使用MEMORY_ONLY
    val lines = ssc.socketTextStream("master",9998,StorageLevel.MEMORY_ONLY)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    //将结果输出到控制台
    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()



  }

  class ClickTrackerKryoRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo): Unit = {
      kryo.register(classOf[TrackerLog])
    }
  }

  case class TrackerLog(id: String, name: String)

}
