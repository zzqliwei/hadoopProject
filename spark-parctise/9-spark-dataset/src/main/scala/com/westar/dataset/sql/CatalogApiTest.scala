package com.westar.dataset.sql

import org.apache.spark.sql.SparkSession

object CatalogApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CatalogApiTest")
      .getOrCreate()


    //查看spark sql应用用的是哪一种catalog
    //目前支持hive metastore 和in-memory两种
    //spark-shell默认的值为hive
    //spark-shell --master spark://master:7077 --conf spark.sql.catalogImplementation=in-memory
    spark.conf.get("spark.sql.catalogImplementation")

    //1：数据库元数据信息
    spark.catalog.listDatabases().show(false)
    spark.catalog.currentDatabase
    val db = spark.catalog.getDatabase("default")
    spark.catalog.databaseExists("zzq")

    spark.sql("CREATE DATABASE IF NOT EXISTS zzq " +
      "COMMENT 'Test database' LOCATION 'hdfs://master:9999/user/hadoop-twq/spark-db'")
    spark.catalog.setCurrentDatabase("twq")
    spark.catalog.currentDatabase

    spark.catalog.listTables("zzq").show()

    //用sql的方式查询表
    val sessionRecords = spark.sql("select * from trackerSession")
    sessionRecords.show()

    spark.catalog.tableExists("log")
    spark.catalog.tableExists("trackerSession")
    spark.catalog.tableExists("twq", "trackerSession") //todo 感觉应该是spark的bug
    spark.catalog.listTables("twq").show()
    spark.catalog.getTable("trackerSession")

    //表的缓存
    spark.catalog.cacheTable("trackerSession")
    spark.catalog.uncacheTable("trackerSession")

    //3：表的列的元数据信息
    spark.catalog.listColumns("trackerSession").show()

    spark.sql("drop table trackerSession")
    spark.sql("drop database zzq")
    spark.catalog.setCurrentDatabase("default")
    spark.catalog.listTables().show()

    spark.stop()


  }

}
