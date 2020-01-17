package com.westar.dataset.sql.udaf

import com.westar.dataset.Order
import org.apache.spark.sql.SparkSession
import com.westar.dataset.Utils._

object GroupApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GroupApiTest")
      .getOrCreate()

    import spark.implicits._

    val orders = spark.read.json(s"${BASE_PATH}/join/orders.json")
    orders.show()

    //1: 基础的聚合函数
    orders.groupBy("userId").count().show()

    orders.groupBy("userId").avg("totalPrice").show()

    orders.groupBy("userId").max("totalPrice").show()

    orders.groupBy("userId").min("totalPrice").show()

    orders.groupBy("userId").sum("totalPrice").show()

    orders.groupBy("userId").mean("totalPrice").show()


    //pivot
    val orderItems = spark.read.json(s"${BASE_PATH}/join/order_items.json")
    orderItems.show()
    orderItems.groupBy("userId").pivot("name").sum("price").show()
    orderItems.groupBy("userId").pivot("name", Seq("apple", "cake")).sum("price").show()

    //2： 用agg来将分组函数聚合起来一起查询
    import org.apache.spark.sql.functions._
    orders.groupBy("userId").agg(
      avg("totalPrice"),
      max("totalPrice"),
      min("totalPrice"),
      sum("totalPrice")
    ).show()

    orders.groupBy("userId").agg(
      "totalPrice" -> "avg",
      "totalPrice" -> "max",
      "totalPrice" -> "min",
      "totalPrice" -> "sum").show()

    //对整个orders进行聚合计算
    orders.agg(
      avg("totalPrice"),
      max("totalPrice"),
      min("totalPrice"),
      sum("totalPrice")).show()

    orders.agg(
      "totalPrice" -> "avg",
      "totalPrice" -> "max",
      "totalPrice" -> "min",
      "totalPrice" -> "sum").show()

    val strDS = Seq("abc", "xyz", "hello").toDS()

    strDS.groupByKey(s => Tuple1(s.length)).count().show()

    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    ds.groupByKey(_._1).agg(sum("_2").as[Long]).show()

    ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]).show()

    ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")).show()

    ds.groupByKey(_._1).agg(
      sum("_2").as[Long],
      sum($"_2" + 1).as[Long],
      count("*").as[Long],
      avg("_2").as[Double]).show()

    spark.stop()
  }

}
