package com.westar.spark.rdd.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, JobID, RecordWriter, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

object MapReduceWriterNewApiTest {

  def main(args: Array[String]): Unit = {
    //1 构建并配置写数据的一个job实例，包括写入key的类型、value的类型以及写入数据的文件格式
    val conf = new Configuration()
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])

    val jobConfiguration = job.getConfiguration
    jobConfiguration.set("mapred.output.dir", "hdfs://master:9999/users/hadoop-twq/test")

    //2 构建job相关的id
    val attemptId = new TaskAttemptID("jobtrackerID", 0, TaskType.REDUCE, 0, 0)
    val hadoopContext = new TaskAttemptContextImpl(jobConfiguration, attemptId)

    //3 获取输出数据的文件类型
    val format = job.getOutputFormatClass.newInstance
    //4 获取输出的committer
    val committer = format.getOutputCommitter(hadoopContext)
    committer.setupTask(hadoopContext)
    //5 获取写文件的writer，并写文件
    val writer = format.getRecordWriter(hadoopContext).asInstanceOf[RecordWriter[NullWritable, Text]]

    val key = null
    val value = "test"
    writer.write(null, new Text("teet"))

    //6 关闭writer
    writer.close(hadoopContext)

    committer.commitTask(hadoopContext)

  }

}
