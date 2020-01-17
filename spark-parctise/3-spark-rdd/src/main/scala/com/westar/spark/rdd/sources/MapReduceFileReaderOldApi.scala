package com.westar.spark.rdd.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, Reporter}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.util.ReflectionUtils

object MapReduceFileReaderOldApi {

  def main(args: Array[String]): Unit = {
    //1 构建配置
    val hadoopConf = new Configuration()
    val jobConf = new JobConf(hadoopConf)

    //2 设置数据存储的文件
    FileInputFormat.setInputPaths(jobConf, "hdfs://master:9999/users/hadoop-twq/word.txt")

    //3 获取读取文件的格式
    val inputFormat = ReflectionUtils.newInstance(classOf[TextInputFormat], jobConf)
      .asInstanceOf[InputFormat[LongWritable, Text]]

    //4 获取需要读取文件的数据块的分区信息
    //4.1 获取文件被分成多少数据块了
    val minSplit = 1
    val inputSplits = inputFormat.getSplits(jobConf, minSplit)
    val firstSplit = inputSplits(0)

    //4.2 获取第一个数据块的存储信息
    val splitInfoReflections = new SplitInfoReflections
    val lsplit = splitInfoReflections.inputSplitWithLocationInfo.cast(firstSplit)
    val preferLocations = splitInfoReflections.getLocationInfo.invoke(lsplit).asInstanceOf[Array[AnyRef]]

    val firstPreferLocation = preferLocations(0)
    val locationStr = splitInfoReflections.getLocation.invoke(firstPreferLocation).asInstanceOf[String]
    val isMemory = splitInfoReflections.isInMemory.invoke(firstPreferLocation).asInstanceOf[Boolean]

    //5 读取第一个数据块的数据
    val reader = inputFormat.getRecordReader(firstSplit, jobConf, Reporter.NULL)
    val key = reader.createKey()
    val value = reader.createValue()

    val finished = !reader.next(key, value)

  }
}

class SplitInfoReflections {
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname
  }

  val inputSplitWithLocationInfo =
    classForName("org.apache.hadoop.mapred.InputSplitWithLocationInfo")
  val getLocationInfo = inputSplitWithLocationInfo.getMethod("getLocationInfo")
  val newInputSplit = classForName("org.apache.hadoop.mapreduce.InputSplit")
  val newGetLocationInfo = newInputSplit.getMethod("getLocationInfo")
  val splitLocationInfo = classForName("org.apache.hadoop.mapred.SplitLocationInfo")
  val isInMemory = splitLocationInfo.getMethod("isInMemory")
  val getLocation = splitLocationInfo.getMethod("getLocation")
}
