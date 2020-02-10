//package com.westar.sql.hive.example
//
//import org.apache.spark.sql.SparkSession
//import org.scalatest.FlatSpec
//
///**
//  * Created by tangweiqun on 2017/11/7.
//  */
//class UserMovieRatingEtlTest extends FlatSpec {
//
//  behavior of "UserMovieRatingEtlTest"
//
//  it should "doETL succ" in {
//    val spark = SparkSession.builder()
//      .appName("UserMovieRatingEtlTest")
//      .enableHiveSupport()
//      .master("local")
//      .getOrCreate()
//
//    spark.catalog.setCurrentDatabase("twq")
//    spark.sql("drop table if exists u_data")
//    spark.sql("drop table if exists u_user")
//    spark.sql("drop table if exists u_item")
//
//    UserMovieRatingEtl.doETL(spark)
//
//    checkRatingData(spark)
//
//    checkUser(spark)
//
//    checkItem(spark)
//
//    spark.stop()
//  }
//
//  private def checkItem(spark: SparkSession) = {
//    val itemDF = spark.read.table("twq.u_item")
//    val items = itemDF.collect()
//    val item1Opt = items.find(row => {
//      row.getAs[Int]("movie_id") == 1
//    })
//    assert(item1Opt.get.getAs[String]("movie_title") == "Toy Story (1995)")
//    assert(item1Opt.get.getAs[String]("release_data") == "01-Jan-1995")
//    assert(item1Opt.get.getAs[String]("video_release_date") == null)
//    assert(item1Opt.get.getAs[String]("imdb_url") == "http://us.imdb.com/M/title-exact?Toy Story (1995)")
//    assert(item1Opt.get.getAs[Int]("unknow") == 0)
//    assert(item1Opt.get.getAs[Int]("action") == 0)
//    assert(item1Opt.get.getAs[Int]("adventure") == 0)
//    assert(item1Opt.get.getAs[Int]("animation") == 1)
//    assert(item1Opt.get.getAs[Int]("children") == 1)
//    assert(item1Opt.get.getAs[Int]("comedy") == 1)
//    assert(item1Opt.get.getAs[Int]("crime") == 0)
//    assert(item1Opt.get.getAs[Int]("documentary") == 0)
//    assert(item1Opt.get.getAs[Int]("drama") == 0)
//    assert(item1Opt.get.getAs[Int]("fantasy") == 0)
//    assert(item1Opt.get.getAs[Int]("film_noir") == 0)
//    assert(item1Opt.get.getAs[Int]("horror") == 0)
//    assert(item1Opt.get.getAs[Int]("musical") == 0)
//    assert(item1Opt.get.getAs[Int]("mystery") == 0)
//    assert(item1Opt.get.getAs[Int]("romance") == 0)
//    assert(item1Opt.get.getAs[Int]("sci_fi") == 0)
//    assert(item1Opt.get.getAs[Int]("thriller") == 0)
//    assert(item1Opt.get.getAs[Int]("war") == 0)
//    assert(item1Opt.get.getAs[Int]("western") == 0)
//  }
//
//  private def checkUser(spark: SparkSession) = {
//    val userDF = spark.read.table("twq.u_user")
//    val users = userDF.collect()
//    assert(users.size == 10)
//    val user1Opt = users.find(row => {
//      row.getAs[Int]("user_id") == 1
//    })
//    assert(user1Opt.get.getAs[Int]("age") == 24)
//    assert(user1Opt.get.getAs[String]("gender") == "M")
//    assert(user1Opt.get.getAs[String]("occupation") == "technician")
//    assert(user1Opt.get.getAs[String]("zip_code") == "85711")
//  }
//
//  private def checkRatingData(spark: SparkSession) = {
//    val ratingDataDF = spark.read.table("twq.u_data")
//    val ratingData = ratingDataDF.collect()
//    assert(ratingData.size == 450)
//
//    val data196Opt = ratingData.find(row => {
//      row.getAs[Int]("user_id") == 196
//    })
//    assert(data196Opt.get.getAs[Int]("item_id") == 242)
//    assert(data196Opt.get.getAs[Int]("rating") == 3)
//    assert("1997-12-04 23:55:49".equals(data196Opt.get.getAs[String]("data_time")))
//    assert(data196Opt.get.getAs[Int]("year") == 1997)
//    assert(data196Opt.get.getAs[Int]("month") == 199712)
//  }
//}
