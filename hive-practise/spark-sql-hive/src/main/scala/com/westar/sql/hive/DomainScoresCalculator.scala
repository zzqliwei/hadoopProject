package com.westar.sql.hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.callUDF

/**
 * spark-submit --class com.westar.sql.hive.DomainScoresCalculator \
 *--master spark://master:7077 \
 *--deploy-mode client \
 *--driver-memory 512m \
 *--executor-memory 512m \
 *--total-executor-cores 2 \
 *--executor-cores 1 \
 * /home/hadoop/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar
 */
object DomainScoresCalculator {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DomainScoresCalculator")
      .enableHiveSupport()
      .getOrCreate()
    val numberPartitions = spark.conf.get("spark.trackerSession.domainScoresCalculator.partition", "2").toInt

    val cookieLabelScoresMap = Map("固执" -> 10,"有偏见" -> 6)

    val cookieLabelScoresMapB = spark.sparkContext.broadcast(cookieLabelScoresMap)

    def getCookieLableScores(cookieLabel: String):Int ={
      cookieLabelScoresMapB.value.getOrElse(cookieLabel,0)
    }

    spark.udf.register("getCookieLableScores",(cookieLabel:String) => getCookieLableScores(cookieLabel))

    import spark.implicits._

    spark.read.table("westar.tracker_session")
      .select($"domain",callUDF("getCookieLabelScores",$"cookie_label").as("cookie_label_scores"))
      .groupBy($"domain")
      .sum("cookie_label_scores")
      .toDF("domain","scores")
      .coalesce(numberPartitions)
      .write
      .mode(SaveMode.Overwrite)
    //.bucketBy(5, "domain") //这里的bucket和hive的bucket功能是一样的，但是不兼容hive的bucket
      .save("westar.domain_scores")

    spark.stop()
  }

}
