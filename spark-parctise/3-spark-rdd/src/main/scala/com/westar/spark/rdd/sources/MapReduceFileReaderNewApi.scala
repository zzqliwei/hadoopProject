package com.westar.spark.rdd.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}

object MapReduceFileReaderNewApi {

  def main(args: Array[String]): Unit = {

    //1 构建一个job实例
    val hadoopConf = new Configuration()
    val job = Job.getInstance(hadoopConf)

    //2 设置需要读取的文件全路径
    FileInputFormat.setInputPaths(job, "hdfs://master:9999/users/hadoop-twq/word.txt")

    //3 获取读取文件的格式
    val inputFormat = classOf[TextInputFormat].newInstance()

    val updateConf = job.getConfiguration

    //4 获取需要读取文件的数据块的分区信息
    //4.1 获取文件被分成多少数据块了
    val minSplit = 1
    val jobId = new JobID("jobTrackerId", 123)
    val jobContext = new JobContextImpl(updateConf, jobId)
    val inputSplits = inputFormat.getSplits(jobContext).toArray
    val firstSplit = inputSplits(0).asInstanceOf[InputSplit]

    //4.2 获取第一个数据块的存储信息
    val splitInfoReflections = new SplitInfoReflections
    val lsplit = splitInfoReflections.inputSplitWithLocationInfo.cast(firstSplit)
    val preferLocations = splitInfoReflections.getLocationInfo.invoke(lsplit).asInstanceOf[Array[AnyRef]]

    val firstPreferLocation = preferLocations(0)
    val locationStr = splitInfoReflections.getLocation.invoke(firstPreferLocation).asInstanceOf[String]
    val isMemory = splitInfoReflections.isInMemory.invoke(firstPreferLocation).asInstanceOf[Boolean]


    //5 读取第一个数据块的数据
    val attemptId = new TaskAttemptID("jobTrackerId", 123, TaskType.MAP, 0, 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(updateConf, attemptId)
    val reader = inputFormat.createRecordReader(firstSplit, hadoopAttemptContext)
    reader.initialize(firstSplit, hadoopAttemptContext)
    val isFirst = reader.nextKeyValue()
    val key = reader.getCurrentKey
    val value = reader.getCurrentValue
  }

}
