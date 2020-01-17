package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * cast类型转换函数
 */
object CastFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CastFunctionTest")
      .master("local")
      .getOrCreate()

    //cast 类型转换
    spark.sql("select cast('10' as int)").show() // 输出类型为int的10

    //cast中的as可以跟以下的类型：
    /*
    castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType)
     */


    spark.stop()
  }
}
