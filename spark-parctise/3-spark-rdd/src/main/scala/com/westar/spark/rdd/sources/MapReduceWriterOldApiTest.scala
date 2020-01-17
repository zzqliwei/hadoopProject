package com.westar.spark.rdd.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.Lz4Codec
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType

object MapReduceWriterOldApiTest {
  def main(args: Array[String]): Unit = {
    //1 设置配置的属性，包括写入key的类型、value的类型以及写入数据的文件格式
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    jobConf.setOutputKeyClass(classOf[NullWritable])
    jobConf.setOutputValueClass(classOf[Text])
    jobConf.setOutputFormat(classOf[TextOutputFormat[NullWritable, Text]])

    //2 写入数据的压缩设置
    val codec = Some(classOf[Lz4Codec])
    for(c <- codec){
      jobConf.setCompressMapOutput(true)
      jobConf.set("mapred.output.compress", "true")
      jobConf.setMapOutputCompressorClass(c)
      jobConf.set("mapred.output.compression.codec", c.getCanonicalName)
      jobConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }
    //3 设置数据写入到的文件夹
    val path = "hdfs://master:9999/users/hadoop-twq/test"
    val tempPath = new Path(path)
    val tempFs = tempPath.getFileSystem(conf)
    val finalOutputPath = tempPath.makeQualified(tempFs.getUri , tempFs.getWorkingDirectory)
    FileOutputFormat.setOutputPath(jobConf, finalOutputPath)

    //4 设置job相关的id等
    val jobId = new JobID("jobtrackerID", 123)
    val jobContext = new JobContextImpl(jobConf,jobId)
    val taId = new TaskAttemptID(new TaskID(jobId,TaskType.MAP, 0),0)
    conf.set("mapred.tip.id", taId.getTaskID.toString)
    conf.set("mapred.task.id", taId.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", 0)
    conf.set("mapred.job.id", jobId.toString)

    //5 设置数据写入文件的文件名称
    val outputName = "part-" + System.currentTimeMillis()
    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    val fs = outputPath.getFileSystem(jobConf)

    //6 构建一个写数据的writer，然后写文件
    val writer = jobConf.getOutputFormat
      .asInstanceOf[OutputFormat[AnyRef, AnyRef]]
      .getRecordWriter(fs, jobConf, outputName, Reporter.NULL)

    val key = null
    val value = "test"
    writer.write(key, value)

    //7 关闭writer
    writer.close(Reporter.NULL)


  }

}
