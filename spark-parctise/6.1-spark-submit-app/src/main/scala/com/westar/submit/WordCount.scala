package com.westar.submit

import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * 感官认识spark-submit命令
 * spark-submit --class com.westar.submi.WordCount \
 * --name  "SimpleWordCount" \
 * --master spark://master:7077
 * --deploy-mode client \
 * --driver-memory 1g \
 * --total-executor-cores 2 \
 * --executor-memoey 512m \
 * --executor-cores 1
 * -- /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 *
 *
 * 2、--master参数
 * ## --master=yarn
 * ## --deploy-mode=client
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submi.WordCount \
 * --name "SimpleWordCount" \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1
 * -- /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * 3、 --deploy-mode参数
 * ## --deploy-mode=cluster
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submi.WordCount \
 * --name "SimpleWordCount" \
 * --master yarn \
 * --deploy-mode cluster \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --num-executor 2 \
 * --executor-cores 1 \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * # --deploy-mode=cluster
 * ##--master=spark stand alone
 * spark-submit --class com.westar.submi.WordCount \
 * --name "SimpleWordCount" \
 * --master spark://master:7077 \
 * --deploy-mode cluster \
 * --driver-memory 512m \
 * --executor-memory 512m \
 * --total-executor-cores 2 \
 * --executor-cores 1 \
 * hdfs://master:9999/users/hadoop-twq/submitapp/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 */
object WordCount {

  private val logger = LoggerFactory.getLogger("WordCount")

  def main(args: Array[String]): Unit = {
    if(args.size !=1 ){
      logger.error("arg for partition number is empty")
      System.exit(-1)
    }

    val numPartitions = args(0).toInt
    logger.info(s"numPartitions ========= ${numPartitions}")

    val conf = new SparkConf();
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    val inputRdd = sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/submitapp/word.txt",
      classOf[TextInputFormat],classOf[LongWritable],classOf[Text])

    val words = inputRdd.flatMap(_._2.toString.split(" "))

    val wordCount = words.map(word => ( word,1L))

    val counts = wordCount.reduceByKey(new HashPartitioner(numPartitions), (x, y) => x + y )

    val path = new Path("hdfs://master:9999/users/hadoop-twq/submitapp/wordcount")

    val configuration = new Configuration()
    configuration.set("fs.defaultFS","hdfs://master:9900")
//    FileSystem.get(sc.hadoopConfiguration).exists(path)

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
