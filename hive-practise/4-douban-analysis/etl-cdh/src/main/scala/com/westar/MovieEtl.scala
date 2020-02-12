package com.westar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
 * /**
 * spark-submit--class com.westar.MovieEtl \
 *--master yarn \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 */home/hadoop/hive-course/etl-cdh-1.0-SNAPSHOT.jar \
 * hdfs://master:8020/user/hadoop/hive-course/douban/movie-video.csv \
 * hdfs://master:8020/user/hadoop/hive-course/douban/parquet
 */
object MovieEtl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("MovieEtl")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val doubanMoviePath = args(0)
    val doubanMovieParquetPath = args(1)

    import sqlContext.implicits._

    sc.textFile(doubanMoviePath)
      .map(line => MovieDetailParser.parse(line))
      .toDF("movieId", "movieName", "year", "directors", "scriptsWriters", "stars", "category",
        "nations", "language", "showtime", "initialReleaseDateMap", "commentNum", "commentScore", "summary")
      .write
      .mode(SaveMode.Overwrite)
      .parquet(doubanMovieParquetPath)

    sc.stop()
  }

  case class Person(name: String, age: Long)

}
