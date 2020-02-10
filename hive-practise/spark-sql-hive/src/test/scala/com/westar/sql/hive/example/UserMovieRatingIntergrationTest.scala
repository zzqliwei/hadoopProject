//package com.westar.sql.hive.example
//
//import java.util.Properties
//
//import org.apache.spark.sql.SparkSession
//import org.scalatest.FlatSpec
//
///**
//  * Created by tangweiqun on 2017/11/7.
//  */
//class UserMovieRatingIntergrationTest extends FlatSpec {
//
//  behavior of "UserMovieRatingIntergrationTest"
//
//  it should "UserMovieRatingIntergrationTest succ" in {
//    val spark = SparkSession.builder()
//      .appName("UserMovieRatingIntergrationTest")
//      .enableHiveSupport()
//      .master("local")
//      .config("spark.driver.host", "localhost")
//      .getOrCreate()
//
//    spark.catalog.setCurrentDatabase("twq")
//    spark.sql("drop table if exists u_data")
//    spark.sql("drop table if exists u_user")
//    spark.sql("drop table if exists u_item")
//
//    UserMovieRatingEtl.doETL(spark)
//
//    ALSExample.doALS(spark)
//
//    val properties = new Properties()
//    properties.put("user", "root")
//    properties.put("password", "root")
//
//    val userRecs =
//      spark.read.jdbc("jdbc:mysql://master:3306/test", "userRecs", properties).collect()
//    assert(userRecs.nonEmpty)
//
//    spark.stop()
//  }
//
//
//}
