package com.westar.sql.hive.example

import java.net.URLDecoder

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark-submit --class com.westar.sql.hive.example.UserMovieRatingEtl
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 2 \
 *--conf spark.user.movie.rating.basePath=hdfs://master:8020/user/hadoop/ml-100
 * /home/hadoop/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar
 */
object UserMovieRatingEtl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UserMovieRatingEtl")
      .config("spark.sql.warehouse.dir","hdfs://master:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    if(!spark.catalog.databaseExists("westar")){
      spark.sql("create database westar")
    }
    doETL(spark)
    spark.stop()
  }

  def doETL(spark: SparkSession) = {
    import spark.implicits._

    val basePath = spark.conf.get("spark.user.movie.rating.basePath","hive-practise/spark-sql-hive/src/test/resources/ml-100")

    val ratingDataDF = spark.read
      .option("sep","\\t")
      .option("inferSchema",true)
      .csv(s"${basePath}/u.data")
      .toDF("user_id","item_id","rating","rate_time")

    val userDF = spark.read
      .option("sep","|")
      .option("inferSchema",true)
      .csv(s"${basePath}/u.user")
      .toDF("user_id","age","gender","occupation","zip_code")

    val movieDF = spark.read
      .option("sep","|")
      .option("inferSchema",true)
      .csv(s"${basePath}/u.item")
      .toDF("movie_id","movie_title","release_data",
        "video_release_date", "imdb_url", "unknow",
        "action", "adventure", "animation", "children", "comedy", "crime", "documentary", "drama",
        "fantasy", "film_noir", "horror", "musical", "mystery", "romance", "sci_fi", "thriller",
        "war", "western")

    import org.apache.spark.sql.functions._
    ratingDataDF.select($"user_id",$"item_id",$"rating",from_unixtime($"rate_time","yyyy-MM-dd HH:mm:ss").as("data_time"),
      substring(from_unixtime($"rate_time","yyyyMMdd"),0,4).cast("int").as("year"),
      substring(from_unixtime($"rate_time","yyyyMMdd"),0,6).cast("int").as("month"))
      .write
      .mode(SaveMode.Append)
      .partitionBy("year","month")
      .saveAsTable("westar.u_data")

    userDF.select($"user_id", $"age", $"gender", $"occupation", $"zip_code".cast("string"))
      .write
      .mode(SaveMode.Overwrite).saveAsTable("westar.u_user")

    spark.udf.register("urlDecode",(url:String) =>{
      if(StringUtils.isNotEmpty(url)){
        URLDecoder.decode(url,"UTF-8")
      }else{
        url
      }
    })

    movieDF.select($"movie_id", $"movie_title", $"release_data", $"video_release_date",
      callUDF("urlDecode", $"imdb_url").as("imdb_url"),
      $"unknow", $"action", $"adventure", $"animation", $"children", $"comedy",
      $"crime", $"documentary", $"drama", $"fantasy", $"film_noir", $"horror",
      $"musical", $"mystery", $"romance", $"sci_fi", $"thriller", $"war", $"western")
      .write.mode(SaveMode.Overwrite).saveAsTable("westat.u_item")
  }

}
