package com.westar.sql.hive.example

import java.util.Properties

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark-submit --class com.westar.sql.hive.example.ALSExample \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 *--jars /home/hadoop/spark-course/spark-dataset/mysql-connector-java-5.1.44-bin.jar \
 * /home/hadoop/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar
 */
object ALSExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ALSExample")
      .config("spark.sql.warehouse.dir", "hdfs://master:9999/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    doALS(spark)

    spark.stop()
  }
  def doALS(spark: SparkSession) = {
    import spark.implicits._
    //1、准备DataFrame
    val ratings = spark.read.table("westar.u_data")
      .select($"user_id",$"item_id",$"rating".cast("float").as("rating"))

    val Array(training,test) = ratings.randomSplit(Array(0.8,0.2))

    //2、ALS算法和训练数据集，产生推荐模型
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("ration")

    val model = als.fit(training)

    //3.0 模型评估，计算RMSE,均方根误差
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    //为每一个用户推荐5部电影
    val userRecs = model.recommendForAllUsers(5)
      .select($"user_id",explode($"recommendations").as("rec"))
      .select($"user_id",$"rec".getField("item_id").as("item_id"))

    val items = spark.read.table("westar.u_item")
      .select("movie_id","movie_title")
    //将结果通过jdbc保存到mysql中
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")


    //如果想在本地测试的话，需要设置依赖mysql的jdbc驱动包
    userRecs.join(items,userRecs.col("item_id") === items.col("movie_id"))
      .select("user_id","item_id","movie_title")
      .coalesce(2)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://master:3306/test", "userRecs", properties)
  }
}
