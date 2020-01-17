package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * 集合函数
 * array
 * array_contains
 * map
 * named_struct
 * map_keys
 * map_values
 * size
 * sort_array
 */
object CollectionFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CollectionFunctionTest")
      .master("local")
      .getOrCreate()

    //array 用法 array(expr, ...) 返回元素expr, ...组成的数组
    spark.sql("select array(1,2,3)").show()

    //array_contains 用法 array_contains(array, value) 如果array中含有value的话则返回true
    spark.sql("select array_contains(array(1,2,3),4)").show()

    //map 用法 map(key0, value0, key1, value1, ...) 用指定的key value创建一个map
    spark.sql("SELECT map(1.0, '2', 3.0, '4')").show(false)

    //named_struct 用法 named_struct(name1, val1, name2, val2, ...) 用给定的域名和值创建一个struct
    // todo
    spark.sql("SELECT named_struct(\"a\", 1, \"b\", 2, \"c\", 3)").show(false) //输出：[1, 2, 3]

    //map_keys 用法 map_keys(map) 返回map中的所有的key
    spark.sql("SELECT map_keys(map(1, 'a', 2, 'b'))").show(false) //输出：[1, 2]

    //map_values 用法 map_values(expr, ...) 返回map中的所有的value
    spark.sql("SELECT map_values(map(1, 'a', 2, 'b'))").show(false) //输出：[a, b]

    //size 用法 size(expr) 返回一个array或者map的长度
    spark.sql("SELECT size(array('b', 'd', 'c', 'a'))").show(false) //输出：4
    spark.sql("SELECT size(map('b', 'd', 'c', 'a'))").show(false) //输出：2

    //size 用法 size(expr) 返回一个array或者map的长度
    spark.sql("SELECT size(array('b', 'd', 'c', 'a'))").show(false) //输出：4

    //sort_array 用法 sort_array(array[, ascendingOrder]) 给array按照desc或者asc进行排序，如果ascendingOrder为true则表示按照asc排序
    spark.sql("SELECT sort_array(array('b', 'd', 'c', 'a'), true)").show(false)


    spark.stop()

  }

}
