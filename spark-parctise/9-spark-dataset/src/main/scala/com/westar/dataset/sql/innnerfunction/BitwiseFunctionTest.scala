package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * 位运算函数
 * & ~ | ^
 */
object BitwiseFunctionTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BitwiseFunctionTest")
      .master("local")
      .getOrCreate()

    //& 按位取与运算
    /*
    00000011   ---> 3
    00000101   ---> 5

    00000001   ---->1
     */

    spark.sql("select 3 & 5 ").show()

    //~ 按位取非运算
    /*
    00000000 00000000 00000000 00000101  -----> 5

    11111111 11111111 11111111 11111010  -----> -6
     */
    spark.sql("select ~(5) ").show()

    //| 按位取或运算
    /*
   00000011   ---> 3
   00000101   ---> 5

   00000111   ---->7
    */
    spark.sql("select 3 | 5").show() //输出为：7

    //^ 按位取异或运算
    /*
   00000011   ---> 3
   00000101   ---> 5

   00000110   ---->6
    */
    spark.sql("select 3 ^ 5").show() //输出为：6


    spark.stop()

  }

}
