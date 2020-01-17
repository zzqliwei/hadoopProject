package com.westar.scheduler

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(LongWritable, Text)] =
      sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/word.txt",
        classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    val rdd2: RDD[String] = rdd1.flatMap(_._2.toString.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map(word => (word, 1))

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(new HashPartitioner(2), (x, y) => x + y)

    rdd4.saveAsTextFile("hdfs://master:9999/users/hadoop-twq/wordcount")

    sc.stop()
  }

}