package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

object MiscNonAggFunctionTest {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder()
      .appName("MiscNonAggFunctionTest")
      .master("local[*]")
      .getOrCreate()

    // abs 绝对值
    spark.sql("select abs(-1)").show()//输出为1

    //coalesce 得到第一个不是null的值
    spark.sql("select coalesce(1, 2)").show() //输出为1
    spark.sql("SELECT coalesce(null, 1, 2)").show() //输出为1
    spark.sql("SELECT coalesce(null, null, 2)").show() //输出为2
    spark.sql("SELECT coalesce(null, null, null)").show() //输出为null


    //explode 将一个数组中的每一个元素变成每一行的值
    spark.sql("select explode(array(10,20))").show()

    /*
    +---+
    |col|
    +---+
    | 10|
    | 20|
    +---+
     */

    //greatest 得到参数中最大的值(参数的个数必须大于2)，如果参数为null则跳过
    //如果参数都是为null，则结果为null
    spark.sql("SELECT greatest(10, 3, 5, 13, -1)").show() //输出为13

    //least 用法：least(expr, ...) - 返回所有参数中最小的值，跳过为null的值
    spark.sql("SELECT least(10, 3, 5, 13, -1)").show() //输出为-1

    //if
    //用法：if(expr1, expr2, expr3) - 如果`expr1`为true, 则返回`expr2`; 否则返回`expr3`.
    spark.sql("SELECT if(1 < 2, 'a', 'b')").show() // 输出为a

    //inline 将结构体的数组转成表
    spark.sql("select inline(array(struct(1,'a'),struct(2,'b')))").show( )
    /*
       +----+----+
       |col1|col2|
       +----+----+
       |   1|   a|
       |   2|   b|
       +----+----+
        */
    //isnan 判断参数是否是NAN(not a number)
    spark.sql("SELECT isnan(cast('NaN' as double))").show() //输出为true

    //ifnull 用法：ifnull(expr1, expr2) - 如果expr1为null则返回expr2，否则返回expr1
    spark.sql("SELECT ifnull(NULL, array('2'))").show() //输出 [2]

    //isnull 用法：isnull(expr) - 如果expr为null则返回true，否则返回false
    spark.sql("SELECT isnull(1)") //输出false

    //isnotnull 用法：isnotnull(expr) - 如果expr不为null则返回true，否则返回false
    spark.sql("SELECT isnotnull(1)") //输出true

    // nanvl 用法： nanvl(expr1, expr2) - 如果expr1不是NAN则返回expr1，否则返回expr2
    spark.sql("SELECT nanvl(cast('NaN' as double), 123)").show() //输出是123.0

    //nullif 用法：nullif(expr1, expr2) - 如果`expr1` 等于 `expr2`则返回null, 否则返回 `expr1`.
    spark.sql("SELECT nullif(2, 2)").show() //返回null

    //nvl 用法：nvl(expr1, expr2) - 如果`expr1` 为 null 则返回expr2, 否则返回`expr1`.
    spark.sql("SELECT nvl(NULL, array('2'))").show() //输出 [2]

    //nvl2 用法：nvl2(expr1, expr2, expr3) - 如果expr1不是null则返回`expr2`, 否则返回`expr3`.
    spark.sql("SELECT nvl2(NULL, 2, 1)").show() //输出 1

    //posexplode
    // 用法：posexplode(expr) -
    // 将array的expr中的每一个元素变成带有位置信息的每一行,
    // 将map的expr中的每一个键值对变成带有位置信息的每一行,
    spark.sql("SELECT posexplode(array(10,20))").show()
    /*
        +---+---+
        |pos|col|
        +---+---+
        |  0| 10|
        |  1| 20|
        +---+---+
         */

    spark.sql("SELECT posexplode(map(1.0, '2', 3.0, '4'))").show()
    /*
    +---+---+-----+
    |pos|key|value|
    +---+---+-----+
    |  0|1.0|    2|
    |  1|3.0|    4|
    +---+---+-----+
     */

    //rand 用法：rand([seed]) - 返回一个随机独立同分布(i.i.d.)的值，取值区间为[0, 1).
    spark.sql("SELECT rand()").show() //输出为0.30627416170191424 每次运行都不一样
    spark.sql("SELECT rand(0)").show() //输出为0.8446490682263027 每次运行都是一样
    spark.sql("SELECT rand(null)").show() //输出为0.8446490682263027 每次运行都是一样

    //randn 用法：randn([seed]) - 返回一个随机独立同分布(i.i.d.)的值，取值的逻辑是符合标准正太分布
    spark.sql("SELECT randn()").show() //输出为-1.499402805473817 每次运行都不一样
    spark.sql("SELECT randn(0)").show() //输出为1.1164209726833079 每次运行都是一样
    spark.sql("SELECT randn(null)").show() //输出为1.1164209726833079 每次运行都是一样


    //stack 用法：stack(n, expr1, ..., exprk) - 将 `expr1`, ..., `exprk` 分成 `n` 行.
    spark.sql("SELECT stack(2, 1, 2, 3)").show()
    /*
    +----+----+
    |col0|col1|
    +----+----+
    |   1|   2|
    |   3|null|
    +----+----+
     */

    //when 用法：CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END
    // - When `expr1` = true, returns `expr2`; when `expr3` = true, return `expr4`; else return `expr5`
    spark.sql("SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 3 THEN 'B' ELSE 'C' END").show() // 输出 C
    
    spark.stop();
  }

}
