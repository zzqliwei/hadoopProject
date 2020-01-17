package com.westar.submit

import java.util.concurrent.TimeUnit

import com.westar.spark.rdd.Dog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * --files and --properties-file
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submit.FilesParamTest \
 * --name "FilesParamTest" \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --jars "spark-rdd-1.0-SNAPSHOT.jar" \
 * --packages org.apache.parquet:parquet-avro:1.8.1 \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --file /home/hadoop-twq/spark-course/wordcount.properties \
 * --properties-file /home/hadoop-twq/spark-course/spark-wordcount.conf \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * --queue
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.twq.submit.FilesParamTest \
 * --name "FilesParamTest" \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --jars "spark-rdd-1.0-SNAPSHOT.jar" \
 * --packages org.apache.parquet:parquet-avro:1.8.1 \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --files wordcount.properties \
 * --properties-file spark-wordcount.conf \
 * --queue hadoop-twq \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * --archives
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.twq.submit.FilesParamTest \
 * --name "FilesParamTest" \
 * --master yarn \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --archives "/home/hadoop-twq/spark-course/submitapp.har/part-0" \
 * --packages org.apache.parquet:parquet-avro:1.8.1 \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --files wordcount.properties \
 * --properties-file spark-wordcount.conf \
 * --queue hadoop-twq \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 */
object FilesParamTest {
  private val logger = LoggerFactory.getLogger("WordCount")

  def main(args: Array[String]): Unit = {
    if (args.size != 1) {
      logger.error("arg for partition number is empty")
      System.exit(-1)
    }

    val numPartitions = args(0).toInt

    val conf = new SparkConf()

    val inputFile = conf.getOption("spark.wordcount.input.file")
      .getOrElse(sys.error("spark.wordcount.input.file must set"))
    logger.info(s"inputFile ========== ${inputFile}")

    val outputPath = conf.getOption("spark.wordcount.output.path")
      .getOrElse(sys.error("spark.wordcount.output.path must set"))
    logger.info(s"outputPath ========== ${outputPath}")

    val hdfsMater = conf.getOption("spark.wordcount.hdfs.master")
      .getOrElse(sys.error("spark.wordcount.hdfs.master must set"))
    logger.info(s"hdfsMater ========== ${hdfsMater}")

    // test driver options中的属性
    val sleepDuration = System.getProperty("sleepDuration","20").toInt
    logger.info(s"sleepDuration ========== ${sleepDuration}")

    val sc = new SparkContext(conf)

    val inputRdd = sc.hadoopFile(inputFile,
      classOf[TextInputFormat],classOf[LongWritable],classOf[Text])

    val words = inputRdd.flatMap(_._2.toString.split(" "))
    val wordCount = words.map( (_,1L))
    val counts = wordCount.reduceByKey(new HashPartitioner(numPartitions), (x, y) => x + y)

    val path = new Path(outputPath)
    val configuration = new Configuration();
    configuration.set("fs.defalutFS",hdfsMater)

    val fs = path.getFileSystem(configuration)
    if(fs.exists(path)){
      fs.delete(path,true)
    }

    counts.saveAsTextFile(path.toString)

    // test driver class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dog1 = new Dog()
    dog1.setName("one")

    val dog2 = new Dog()
    dog2.setName("two")

    val dog3 = new Dog()
    dog3.setName("three")

    //睡sleepDuration秒，为了我们查看进程
    TimeUnit.SECONDS.sleep(sleepDuration)
    sc.stop()

  }

}
