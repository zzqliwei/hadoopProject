package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * 对比函数
 * =
 * ==
 * <=> 用法，如果字段值不为null的话，则功能和=是一样的
 * 如果比较的两个字段的值都是null的话则返回true，如果有一个字段的值为null的话则返回false
 *
 * >
 * >=
 * <
 * <=
 *
 * !(  <= )
 *
 */
object ComparisonFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PredicatesFunctionTest")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val numberDF = Seq(10, 40, 40, 50, 50, 60, 90, 90).toDF("score")

    numberDF.createOrReplaceTempView("numbers")

    //= 和 ==功能是一样的
    spark.sql("select * from numbers where score = 40").show()
    spark.sql("select * from numbers where score == 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   40|
    |   40|
    +-----+
     */

    //<=> 用法，如果字段值不为null的话，则功能和=是一样的
    //如果比较的两个字段的值都是null的话则返回true，如果有一个字段的值为null的话则返回false
    spark.sql("select * from numbers where score <=> 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   40|
    |   40|
    +-----+
     */
    spark.sql("select * from numbers where null <=> null").show()
    /*
    +-----+
    |score|
    +-----+
    |   10|
    |   40|
    |   40|
    |   50|
    |   50|
    |   60|
    |   90|
    |   90|
    +-----+
     */
    spark.sql("select * from numbers where score <=> null").show()
    /*
    +-----+
    |score|
    +-----+
    +-----+
     */


    spark.sql("select * from numbers where score > 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   50|
    |   50|
    |   60|
    |   90|
    |   90|
    +-----+
     */

    spark.sql("select * from numbers where score >= 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   40|
    |   40|
    |   50|
    |   50|
    |   60|
    |   90|
    |   90|
    +-----+
     */

    spark.sql("select * from numbers where score < 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   10|
    +-----+
     */

    spark.sql("select * from numbers where score <= 40").show()
    /*
    +-----+
    |score|
    +-----+
    |   10|
    |   40|
    |   40|
    +-----+
     */

    spark.sql("select * from numbers where !(score <= 40)").show()
    /*
    +-----+
    |score|
    +-----+
    |   50|
    |   50|
    |   60|
    |   90|
    |   90|
    +-----+
     */

    spark.stop()
  }
}