package com.westar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object ALSExample {

  case class Rating(userId: Int,movieId: Int,rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val field = str.split("::")
    Rating(field(0).toInt,field(1).toInt,field(0).toFloat,field(0).toLong)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .appName("ALSExample")
        .getOrCreate()
     import spark.implicits._

    val ratings = spark.read.textFile("hdfs://hadoop0:9000/user/hadoop/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()

    val Array(training, test) = ratings.randomSplit(Array(0.8,0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)
    //4： 利用测试数据集来测试推荐模型的效果
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val userRecs = model.recommendForAllUsers(10)

    val movieRecs = model.recommendForAllItems(10)

    userRecs.show()

    movieRecs.show()

    spark.stop()
  }

}
