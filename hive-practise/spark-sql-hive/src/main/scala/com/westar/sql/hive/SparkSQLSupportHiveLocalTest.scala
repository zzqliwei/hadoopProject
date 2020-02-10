package com.westar.sql.hive

import java.io.File
import org.apache.spark.sql.{Row, SparkSession}

object SparkSQLSupportHiveLocalTest {

  def main(args: Array[String]): Unit = {
    //如果在没有部署hive的场景下，spark sql也是可以打开支持hive的开关的
    //这个时候程序会自动做如下两件事：
    // 1：在本地创建一个metastore_db文件目录，然后启动一个derby数据库用来存放数据库的元数据
    // 2：在本地创建一个spark.sql.warehouse.dir指定的文件目录，用来存放数据
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("SparkSQLSupportHiveTest")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE DATABASE IF NOT EXISTS westar")

    sql(
      """
        |CREATE TABLE IF NOT EXISTS westar.tracker_session (
        | session_id string,
        | session_server_time string,
        | cookie string,
        | cookie_label string,
        | ip string,
        | landing_url string,
        | pageview_count int,
        | click_count int,
        | domain string,
        | domain_label string)
        |STORED AS PARQUET
      """.stripMargin)

    sql("LOAD DATA LOCAL INPATH 'hive-practise/spark-sql-hive/src/main/resources/trackerSession' OVERWRITE INTO TABLE westar.tracker_session")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM westar.tracker_session").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM westar.tracker_session").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("select cookie, count(*) as cnt from westar.tracker_session group by cookie")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(cookie: String, count: Long) => s"cookie: $cookie, count: $count"
    }
    stringsDS.show()

    spark.stop()
  }
}
