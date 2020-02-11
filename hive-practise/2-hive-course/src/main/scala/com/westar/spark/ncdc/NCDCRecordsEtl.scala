package com.westar.spark.ncdc

import com.twq.spark.ncdc.{NcdcRecordDto, NcdcRecordParser}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

/**
 * spark-submit --class com.westar.spark.ncdc.NCDCRecordsEtl \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 * /home/hadoop/hive-course/hive-course-1.0-SNAPSHOT.jar
 */
object NCDCRecordsEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NCDCRecordsEtl")
      .enableHiveSupport()
      .getOrCreate()

    spark.read.textFile("hdfs://master:8020/user/hadoop/ncdc/rawdata/records")
      .flatMap(line => Option(NcdcRecordParser.fromLine(line)))(Encoders.bean(classOf[NcdcRecordDto]))
      .write
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ncdc.ncdc_records")

    spark.stop()
  }

}
