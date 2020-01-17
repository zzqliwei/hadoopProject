package com.westar.scheduler

import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * ## --master=spark standalone
 * ## --deploy-mode=client
 * spark-submit --class com.twq.scheduler.TestExternalShuffleService \
 * --name "TestExternalShuffleService" \
 * --master spark://master:7077 \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --total-executor-cores 2 \
 * --executor-cores 1 \
 * --conf spark.dynamicAllocation.enabled=true \
 * --conf spark.shuffle.service.enabled=true \
 * /home/hadoop-twq/spark-course/spark-scheduler-1.0-SNAPSHOT.jar \
 * 2
 *
 * 2、--master参数
 * ## --master=yarn
 * ## --deploy-mode=client
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.twq.scheduler.TestExternalShuffleService \
 * --name "yarn-TestExternalShuffleService" \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --conf spark.dynamicAllocation.enabled=true \
 * --conf spark.shuffle.service.enabled=true \
 * /home/hadoop-twq/spark-course/spark-scheduler-1.0-SNAPSHOT.jar \
 * 2
 *
 */
object TestExternalShuffleService {
  private val logger = LoggerFactory.getLogger("WordCount")

  def main(args: Array[String]): Unit = {

    if (args.size != 1) {
      logger.error("arg for partition number is empty")
      System.exit(-1)
    }

    val numPartitions = args(0).toInt

    logger.info(s"numPartitions ========= ${numPartitions}")

    val conf = new SparkConf()

    //conf.setAppName("word count")

    val sc = new SparkContext(conf)

    val inputRdd: RDD[(LongWritable, Text)] = sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/submitapp/word.txt",
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    val words: RDD[String] = inputRdd.flatMap(_._2.toString.split(" "))

    val wordCount: RDD[(String, Int)] = words.map(word => (word, 1))

    val counts: RDD[(String, Int)] = wordCount.reduceByKey(new HashPartitioner(numPartitions), (x, y) => x + y)

    val path = new Path("hdfs://master:9999/users/hadoop-twq/submitapp/wordcount")

    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "hdfs://master:9999")

    val fs = path.getFileSystem(configuration)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    counts.saveAsTextFile(path.toString)

    //睡20秒，为了我们查看进程
    TimeUnit.SECONDS.sleep(20)
    sc.stop()
  }

}