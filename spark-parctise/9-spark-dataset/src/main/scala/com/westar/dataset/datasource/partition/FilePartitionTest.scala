package com.westar.dataset.datasource.partition

import com.westar.dataset.Utils
import org.apache.spark.sql.{SaveMode, SparkSession}

object FilePartitionTest {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("FilePartitionTest")
      .getOrCreate()

    val sessions = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession")
    sessions.show()
    sessions.printSchema()

    sessions.createOrReplaceTempView("non_partition_table")

    spark.sql("select * from non_partition_table where day = 20170903").show()

    //对数据按照年月日进行分区
    sessions.write.mode(SaveMode.Overwrite).partitionBy("cookie").parquet(s"${Utils.BASE_PATH}/trackerSession_partition")

    val partitionDF = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession_partition")
    partitionDF.show()
    partitionDF.printSchema()

    //用sql查询某20170903这天的数据
    partitionDF.createOrReplaceTempView("partition_table")
    spark.sql("select * from partition_table where cookie='cookie1'").show()

    //取20170903这天的数据
    val day03DF = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession_partition/year=2017/month=201709/day=20170903")
    day03DF.show()
    day03DF.printSchema()

    //bucket只能用于hive表中
    //而且只用于parquet、json和orc文件格式的文件数据
    sessions.write
      .partitionBy("year")
      .bucketBy(24, "cookie")
      .saveAsTable("session")



    spark.stop()
  }

}
