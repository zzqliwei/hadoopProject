package com.westar.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object Text2PartitionParquetFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Text2PartitionParquetFile")
      .master("local[*]")
      .getOrCreate()

    spark.read.csv("hdfs://master:8020/user/hadoop/hive-course/omneo.csv")
      .toDF("id", "event_id", "event_type", "part_name", "part_number", "version", "payload")
      .write.mode(SaveMode.Overwrite)
      .parquet("hdfs://master:8020/user/hadoop/hive-course/omneo-new/year=2018/month=201805/day=20180509")
    spark.stop()
  }

}
