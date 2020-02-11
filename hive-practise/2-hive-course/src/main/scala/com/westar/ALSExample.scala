package com.westar

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
 * spark-submit --class com.westar.ALSExample \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-momory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 *spark-wordcount-1.0-SNAPSHOT.jar
 */
object ALSExample {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    var filePath = "hive-practise\\2-hive-course\\src\\main\\resources\\sample_movielens_ratings.txt"

    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local")
    }else{
      filePath = "hdfs://master:9999/user/hadoop/sample_movielens_ratings.txt"
    }
    val spark = SparkSession
      .builder()
      .appName("ALSExample")
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    //1：读取HDFS上的数据，并解析成Rating对象


    val ratings = spark.read.textFile(filePath)
      .map(parseRating)
      .toDF()
    //2：将数据切分成训练数据集和测试数据集
    val Array(training,test) = ratings.randomSplit(Array(0.8,0.2))
    //3：利用训练数据集构建推荐模型
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    //5：为每一个用户推荐10个电影
    val userRecs = model.recommendForAllUsers(10)
    //6：为每一个电影推荐10个用户
    val movieRecs = model.recommendForAllItems(10)

    userRecs.show()
    movieRecs.show()

    spark.stop()




  }


}
