package com.westar.douban

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark-submit --class  com.westar.douban.MovieEtl \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executors-cores 1 \
 *--conf spark.doban.movie.path=hdfs://master:8020/user/hadoop/hive-course/douban/movie.csv \
 * /home/hadoop/hive-course/hive-course-1.0-SNAPSHOT.jar douban movie
 */
object MovieEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MovieEtl")
      .enableHiveSupport()
      .getOrCreate()
    val movieDataPath = spark.conf.get("spark.douban.movie.path",
      "hdfs://master:8020/user/hadoop/hive-course/douban/movie.csv")

    val db = args(0)
    val movieTable = args(1)

    import spark.implicits._
    spark.sparkContext
      .textFile(movieDataPath)
      .map(line => MovieDataParser.parseline(line)).toDS()
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year")
      .saveAsTable(s"${db}.${movieTable}")

    spark.stop()
  }

}
