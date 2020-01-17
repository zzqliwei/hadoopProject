package com.westar.submit

import java.util.concurrent.TimeUnit

import com.westar.spark.rdd.Dog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submit.ExecutorParamTest \
 * --name "ExecutorParamTest" \
 * --master yarn \
 * --deploy-mode client \
 * --dirver-memory 512m \
 * --conf "spark.driver.extraJavaOptions=-XX:PrintGCDetails -XX:PrintGCTimeStamps -DsleepDuration=3" \
 * --driver-class-path "spark-rdd-1.0-SNAPSHOT.jar" \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
 * -DmaxFavoriteNumber=50 -DdogFavoriteColor=yellow" \
 * --conf spark.wordcount.dog.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/dog \
 * --conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
 * --conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
 * --conf spark.wordcount.hdfs.master=hdfs://master:9999 \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submit.ExecutorParamTest \
 * --name "ExecutorParamTest" \
 * --master yarn \
 * --deploy-mode client \
 * --dirver-memory 512m \
 * --conf "spark.driver.extraJavaOptions=-XX:PrintGCDetails -XX:PrintGCTimeStamps -DsleepDuration=3" \
 * --jars "/home/hadoop-twq/spark-course/spark-rdd-1.0-SNAPSHOT.jar" \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
 * -DmaxFavoriteNumber=50 -DdogFavoriteColor=yellow" \
 * --conf spark.wordcount.dog.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/dog \
 * --conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
 * --conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
 * --conf spark.wordcount.hdfs.master=hdfs://master:9999 \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
 * spark-submit --class com.westar.submit.ExecutorParamTest \
 * --name "ExecutorParamTest" \
 * --master yarn \
 * --deploy-mode cluser \
 * --dirver-memory 512m \
 * --conf "spark.driver.extraJavaOptions=-XX:PrintGCDetails -XX:PrintGCTimeStamps -DsleepDuration=3" \
 * --jars "/home/hadoop-twq/spark-course/spark-rdd-1.0-SNAPSHOT.jar" \
 * --executor-memory 512m \
 * --num-executors 2 \
 * --executor-cores 1 \
 * --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
 * -DmaxFavoriteNumber=50 -DdogFavoriteColor=yellow" \
 * --conf spark.wordcount.dog.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/dog \
 * --conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
 * --conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
 * --conf spark.wordcount.hdfs.master=hdfs://master:9999 \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 * --jars --master spark://master:7077
 * spark-submit --class com.twq.submit.ExecutorParamTest \
 * --name "ExecutorParamTest" \
 * --master spark://master:7077 \
 * --deploy-mode client \
 * --driver-memory 512m \
 * --conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -DsleepDuration=1" \
 * --jars "/home/hadoop-twq/spark-course/spark-rdd-1.0-SNAPSHOT.jar" \
 * --executor-memory 512m \
 * --total-executor-cores 2 \
 * --executor-cores 1 \
 * --conf spark.executor.extraJavaOptions="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
 * -DmaxFavoriteNumber=50 -DdogFavoriteColor=yellow" \
 * --conf spark.wordcount.dog.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/dog \
 * --conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
 * --conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
 * --conf spark.wordcount.hdfs.master=hdfs://master:9999 \
 * /home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 * --jars --master spark://master:6066 --deploy-mode cluster
 * spark-submit --class com.twq.submit.ExecutorParamTest \
 * --name "ExecutorParamTest" \
 * --master spark://master:6066 \
 * --deploy-mode cluster \
 * --driver-memory 512m \
 * --driver-class-path "/home/hadoop-twq/spark-course/spark-rdd-1.0-SNAPSHOT.jar" \
 * --conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -DsleepDuration=1" \
 * --jars "/home/hadoop-twq/spark-course/spark-rdd-1.0-SNAPSHOT.jar" \
 * --executor-memory 512m \
 * --total-executor-cores 2 \
 * --executor-cores 1 \
 * --conf spark.executor.extraJavaOptions="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
 * -DmaxFavoriteNumber=50 -DdogFavoriteColor=yellow" \
 * --conf spark.wordcount.dog.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/dog \
 * --conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
 * --conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
 * --conf spark.wordcount.hdfs.master=hdfs://master:9999 \
 * hdfs://master:9999/users/hadoop-twq/submitapp/spark-submit-app-1.0-SNAPSHOT.jar \
 * 2
 *
 *
 */
object ExecutorParamTest {
  private val logger = LoggerFactory.getLogger("WordCount")

  def buildDogs(dogOutputPath: String, hdfsMater: String, counts: RDD[(String, Long)]) = {
    // test executor class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dogRDD = counts.map{case (word, count) =>
      val dog = new Dog()
      dog.setName(word)
      val maxCount = System.getProperty("maxFavoriteNumber").toInt
      logger.info(s"maxFavoriteNumber from wordcount.properties is ${maxCount}")
      val finalNumber = if (count > maxCount) maxCount else count
      dog.setFavoriteNumber(finalNumber.toInt)
      // test executor java options 属性值
      val dogFavoriteColor = System.getProperty("dogFavoriteColor", "red")
      logger.info(s"dogFavoriteColor from wordcount.properties is ${dogFavoriteColor}")
      dog.setFavoriteColor(dogFavoriteColor)
      dog
    }

    dogRDD.saveAsTextFile(deleteExistFile(dogOutputPath,hdfsMater).toString)
  }

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

    val dogOutputPath = conf.getOption("spark.wordcount.dog.output.path")
      .getOrElse(sys.error("spark.wordcount.dog.output.path must set"))
    logger.info(s"dogOutputPath ========== ${dogOutputPath}")

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

    val path = deleteExistFile(outputPath,hdfsMater)
    counts.saveAsTextFile(path.toString)

    countDogs

    buildDogs(dogOutputPath, hdfsMater, counts)

    //睡sleepDuration秒，为了我们查看进程
    TimeUnit.SECONDS.sleep(sleepDuration)
    sc.stop()
  }

  private def deleteExistFile(outputPath:String,hdfsMater:String ):Path = {
    val path = new Path(outputPath)
    val configuration = new Configuration();
    configuration.set("fs.defalutFS",hdfsMater)

    val fs = path.getFileSystem(configuration)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    path
  }

  private def countDogs = {
    // test driver class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dog1 = new Dog()
    dog1.setName("one")

    val dog2 = new Dog()
    dog2.setName("two")

    val dog3 = new Dog()
    dog3.setName("three")
  }


}
