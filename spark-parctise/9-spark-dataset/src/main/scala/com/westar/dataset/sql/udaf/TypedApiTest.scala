package com.westar.dataset.sql.udaf

import com.westar.dataset.TrackerSession
import com.westar.dataset.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession, TypedColumn}

object TypedApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TypedApiTest")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //1：api和RDD的api比较
    val sessionDS = spark.read.parquet(s"${BASE_PATH}/trackerSession").as[TrackerSession]
    sessionDS.show()

    val sessionRDD = sessionDS.rdd
    val filterRDD:RDD[String] = sessionRDD.map(_.cookie).filter(_ !="cookie1")
    filterRDD.foreach(println(_))

    val filterDS:Dataset[String] = sessionDS.map(_.cookie).filter(_ !="cookie1")
    filterDS.show()

    val rddCookieCounts = sessionRDD.groupBy(_.cookie).map(c => (c._1, c._2.size))
    rddCookieCounts.collect()

    val dsCookieCounts = sessionDS.groupByKey(_.cookie).count()
    dsCookieCounts.show()

    //2：其他Typed API
    sessionDS.mapPartitions(iter => {
      iter.map(_.cookie)
    })
    sessionDS.flatMap(_.ip.split("\\."))

    sessionDS.foreach(s => println(s.ip))

    sessionDS.foreachPartition(iter => iter.foreach(s => println(s.ip)))

    //3 Column转成TypedColumn
    val typedNameColumn: TypedColumn[Any, String] = col("cookie").as[String]
    val colAlias: Column = typedNameColumn.as("cookie")

    //4 查询 TypedColumn , 返回Dataset[String]
    val result: Dataset[String] = sessionDS.select(typedNameColumn)
    result.show()

    val resultDF: DataFrame = sessionDS.select(colAlias)
    resultDF.show()

    //Dataset也支持DSL API
    import spark.implicits._
    val logDf = spark.read.parquet(s"${BASE_PATH}/trackerLog").as("tl")
    val aliasSession = sessionDS.as("ts")
    aliasSession.join(logDf, "cookie")
      .where($"ts.cookie_label" === "固执" and $"tl.log_type" === "pageview")
      .select("tl.url")
      .groupBy("tl.url").count()
      .orderBy($"url".desc)
      .show()

    spark.stop()

  }

}
