package com.westar.dataset

import org.apache.spark.sql.SparkSession

object HighLevelAbstractionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HighLevelAbstractionTest")
      .getOrCreate()

    import spark.implicits._

    val sessionDS = spark.read.parquet(s"${Utils.BASE_PATH}/example/trackerSession")
      .as[TrackerSession]

    val sessionRDD = sessionDS.rdd

    //用RDD的api来计算每一个cookie_label的pageview_count的平均值
    sessionRDD.map( p =>(p.cookie_label,(p.pageview_count,1)))
      .reduceByKey( (pvCount1,pvCount2) => ( pvCount1._1 + pvCount2._1,pvCount1._2 + pvCount2._2 ) )
      .map(pvCounts => (pvCounts._1,pvCounts._2._1 / pvCounts._2._2))
      .collect()

    //用DataFrame的api来计算每一个cookie_label的pageview_count的平均值
    sessionDS.groupBy("cookie_label").avg("pageview_count").show()

    //用RDD的api分别求cookie_lable的pvcount的最小值、最大值、平均值以及总数
    val cookieLablePvCountRDD = sessionRDD
      .map(session => (session.cookie_label,session.pageview_count))
      .cache()

    val minPvCountBycookieLable = cookieLablePvCountRDD.reduceByKey(scala.math.min(_,_))
    minPvCountBycookieLable.collect()

    val maxPvCountBycookieLable = cookieLablePvCountRDD.reduceByKey(scala.math.max(_,_))
    maxPvCountBycookieLable.collect()

    val avgPvCountBycookieLable = cookieLablePvCountRDD.mapValues(x =>(x,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    avgPvCountBycookieLable.collect()

    //用DataFrame的api分别求cookie_lable的pvcount的最小值、最大值、平均值以及总数
    import org.apache.spark.sql.functions._
    val statisticsByCookieLable = sessionDS.groupBy("cookie_label").agg(
      min("pageview_count").as("min_pv_count"),
      max("pageview_count").as("max_pv_count"),
      avg("pageview_count").as("avg_pv_count"),
      count("pageview_count").as("count_pv_count")
    )

    val finalDF = statisticsByCookieLable.withColumn("age_delta",
      statisticsByCookieLable("max_pv_count") - statisticsByCookieLable("min_pv_count"))
    finalDF.show()

    //使用RDD的场景
    def createCombiner = (value:Int) => (value,1)
    def mergeValue = (acc:(Int,Int),value:Int) =>(acc._1 + value, acc._2 + 1)
    def mergeCombiners = (acc1: (Int, Int), acc2: (Int, Int)) =>
      (acc1._1 + acc2._1, acc1._2 + acc2._2)

    sessionRDD.map(s =>(s.cookie,s.click_count))
      .combineByKey(createCombiner,mergeValue,mergeCombiners)
    sessionDS.groupByKey(_.cookie_label)

    //处理非结构化数据
//    spark.sparkContext.sequenceFile("")
//    spark.sparkContext.textFile("")

    spark.stop()

  }

}
