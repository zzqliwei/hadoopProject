package com.westar

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark-submit--class com.westar.MovieEtl \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 *--conf spark.douban.movie.path=hdfs://master:9999/user/hadoop/hive-course/douban/movie-video.csv \
 * /home/hadoop/hive-course/etl-1.0-SNAPSHOT.jar douban movie
 */
object MovieEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieEtl")
      .enableHiveSupport()
      .getOrCreate()

    val db = args(0)
    val table = args(1)

    import spark.implicits._

    val doubanMoviePath = spark.conf.get("spark.douban.movie.path",
      "hdfs://master:8020/user/hadoop/hive-course/douban/movie-video.csv")

    spark.sparkContext.textFile(doubanMoviePath)
      .map(line => MovieDetailParser.parse(line)).toDS()
      .write
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${db}.${table}")

    spark.stop()
  }

}
