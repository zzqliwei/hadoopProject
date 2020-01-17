package com.westar.dataset.sql.udaf

import com.westar.dataset.{Order, User}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import com.westar.dataset.Utils._

object JoinApiTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JoinApiTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val users = spark.read.json(s"${BASE_PATH}/join/users.json")
    users.show()

    val orders = spark.read.json(s"${BASE_PATH}/join/orders.json")
    orders.show()

    val orderItems = spark.read.json(s"${BASE_PATH}/join/order_items.json")
    orderItems.show()

    //1：两张表有相同字段名的join
    //两张表有相同的join字段名的inner join
    users.join(orders,"userId").show()

    //两张表有相同的join字段名的inner join，使的两个字段都只出现一次
    users.join(orders, Seq("userId", "userName")).show()

    //joinType: inner、outer、left_outer、right_outer、leftsemi、leftanti
    //两张表有相同的join字段名的outer join，使的两个字段都只出现一次
    users.join(orders, Seq("userId", "userName"), "outer").show()

    users.join(orders, Seq("userId", "userName"), "left_outer").show()

    users.join(orders, Seq("userId", "userName"), "right_outer").show()


    //查询出users中的userId在orders存在的users
    users.join(orders, Seq("userId", "userName"), "leftsemi").show()

    //和leftsemi相反，查询出users中的userId不在orders存在的users
    users.join(orders, Seq("userId", "userName"), "leftanti").show()

    //2: 两张表中不是根据相同字段名的join，即根据条件来join的
    //两张表没有相同的join字段名的inner join
    val joinResult:Dataset[Row] = orders.join(orderItems,orders("id")===orderItems("orderId"))
    joinResult.show()

    orders.join(orderItems, orders("id") === orderItems("orderId"), "outer").show()


    //3:joinWith 会返回一个二元组类型的Dataset，
    // 二元组的第一个元素的类型是第一个Dataset中的类型，第二个元素的类型是第二个Dataset中的类型
    val joinWithResult: Dataset[(Row, Row)] =
    orders.joinWith(orderItems, orders("id") === orderItems("orderId"))
    joinWithResult.show()

    val userDS = users.as[User]
    val orderDS = orders.as[Order]
    val dsJoinWithResult: Dataset[(User, Order)] =
      userDS.joinWith(orderDS, userDS("userId") === orderDS("userId"))
    dsJoinWithResult.show()

    val dsJoinResult:Dataset[Row] = userDS.join(orderDS, "userId")
    dsJoinResult.show()

    // joinWith也支持不同类型的joinType: inner、outer、left_outer、right_outer、leftsemi、leftanti
    userDS.joinWith(orderDS, userDS("userId") === orderDS("userId"), "outer").show()

    //3:笛卡尔关联
    users.crossJoin(orders).show()

    spark.stop()
  }

}
