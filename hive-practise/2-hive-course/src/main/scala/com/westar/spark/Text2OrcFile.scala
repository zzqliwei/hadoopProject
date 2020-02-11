package com.westar.spark

import org.apache.spark.sql.SparkSession

object Text2OrcFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Text2OrcFile")
      .master("local[*]")
      .getOrCreate()

    spark.read.csv("hdfs://master:8020/user/hadoop/hive-course/omneo.csv")
      .toDF("id", "event_id", "event_type", "part_name", "part_number", "version", "payload")
      .write.orc("hdfs://master:8020/user/hadoop/hive-course/orc")
    spark.stop()
  }

}
