package com.westar.streaming.receiver

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Counts words in new text files created in the given directory
 * 1、监控目录下的文件的格式必须是统一的
 * 2、不支持嵌入文件目录
 * 3、一旦文件移动到这个监控目录下，是不能变的，往文件中追加的数据是不会被读取的
 * spark-shell --master spark://master:7077 --total-executor-cores 4 --executor-cores 2
 * hadoop fs -copyFromLocal test1-process.txt hdfs://master:9999/user/hadoop-twq/spark-course/streaming/filestream
 */
object HdfsFileStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HdfsFileStream")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val filePath = "hdfs://master:9999/user/hadoop-twq/spark-course/streaming/filestream"

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created

    // filePath 表示监控的文件目录
    // filter(Path => Boolean) 表示符合条件的文件路径
    // isNewFile 表示streaming app启动的时候是否需要处理已经存在的文件

    val lines = ssc.fileStream[LongWritable,Text,TextInputFormat](filePath,
      (path:Path) => path.toString.contains("process"),false)
      .map(_._2.toString)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map((_,1)).reduceByKey( _ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()


    ssc.stop(false)


    val linesWithText = ssc.textFileStream(filePath)


    
  }

}
