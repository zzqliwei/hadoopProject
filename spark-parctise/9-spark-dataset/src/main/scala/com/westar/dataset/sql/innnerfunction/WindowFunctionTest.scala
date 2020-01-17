package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

object WindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WindowFunctionTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq("c001,j2se,t002", "c002,java web,t002",
      "c003,ssh,t001", "c004,oracle,t001", "c005,spark,t003", "c006,c,t003", "c007,js,t002")

    val df = spark.read.csv(dataSeq.toDS()).toDF("cno", "cname", "tno")
    df.createOrReplaceTempView("course")

    //LAG 上一个
    spark.sql("SELECT c.*,LAG(c.cname,1) OVER(ORDER BY c.cno) as lag_result FROM course c").show()

    /*
    +----+--------+----+----------+
    | cno|   cname| tno|lag_result|
    +----+--------+----+----------+
    |c001|    j2se|t002|      null|
    |c002|java web|t002|      j2se|
    |c003|     ssh|t001|  java web|
    |c004|  oracle|t001|       ssh|
    |c005|   spark|t003|    oracle|
    |c006|       c|t003|     spark|
    |c007|      js|t002|         c|
    +----+--------+----+----------+
     */
    //LEAD 下一个
    spark.sql("SELECT c.*,LEAD(c.cname,1) OVER(ORDER BY c.cno) as lead_result FROM course c").show()
    /*
    +----+--------+----+-----------+
    | cno|   cname| tno|lead_result|
    +----+--------+----+-----------+
    |c001|    j2se|t002|   java web|
    |c002|java web|t002|        ssh|
    |c003|     ssh|t001|     oracle|
    |c004|  oracle|t001|      spark|
    |c005|   spark|t003|          c|
    |c006|       c|t003|         js|
    |c007|      js|t002|       null|
    +----+--------+----+-----------+
     */
    //row_number 为查询出来的每一行记录生成依次排序且不重复的的序号
    //先使用over子句中的排序语句对记录进行排序，然后按照这个顺序生成序号。over子句中的order by子句与SQL语句中的order by子句没有任何关系，这两处的order by 可以完全不同
    spark.sql("SELECT c.*,row_number() OVER(ORDER BY c.cno) as rowNo FROM course c").show()
    /*
    +----+--------+----+-----+
    | cno|   cname| tno|rowNo|
    +----+--------+----+-----+
    |c001|    j2se|t002|    1|
    |c002|java web|t002|    2|
    |c003|     ssh|t001|    3|
    |c004|  oracle|t001|    4|
    |c005|   spark|t003|    5|
    |c006|       c|t003|    6|
    |c007|      js|t002|    7|
    +----+--------+----+-----+
     */

    val numberDF = Seq(10, 40, 40, 50, 50, 60, 90, 90).toDF("score")
    numberDF.createOrReplaceTempView("numbers")

    //rank函数用于返回结果集的分区内每行的排名， 行的排名是相关行之前的排名数加一

    //dense_rank函数的功能与rank函数类似，dense_rank函数在生成序号时是连续的，
    // 而rank函数生成的序号有可能不连续。dense_rank函数出现相同排名时，将不跳过相同排名号，rank值紧接上一次的rank值
    //在各个分组内，rank()是跳跃排序，有两个第一名时接下来就是第四名，dense_rank()是连续排序，有两个第一名时仍然跟着第二名

    //cume_dist的计算方法：小于等于当前行值的行数/总行数。
    //percent_rank的计算方法：当前rank值-1/总行数-1

    //ntile函数可以对序号进行分组处理，将有序分区中的行分发到指定数目的组中

    //参考：http://www.cnblogs.com/52XF/p/4209211.html

    spark.sql(
      """
        |select ROW_NUMBER() over(order by score) as rownum
        |,score
        |,cume_dist()over(order by score) as cum
        |,percent_rank() over(order by score) as per_rnk
        |,rank() over(order by score) as rnk
        |,dense_rank() over(order by score) as dense_rnk
        |,ntile(4) over(order by score) as nt
        |from numbers
      """.stripMargin).show()
    /*
    +------+-----+-----+-------------------+---+---------+---+
    |rownum|score|  cum|            per_rnk|rnk|dense_rnk| nt|
    +------+-----+-----+-------------------+---+---------+---+
    |     1|   10|0.125|                0.0|  1|        1|  1|
    |     2|   40|0.375|0.14285714285714285|  2|        2|  1|
    |     3|   40|0.375|0.14285714285714285|  2|        2|  2|
    |     4|   50|0.625|0.42857142857142855|  4|        3|  2|
    |     5|   50|0.625|0.42857142857142855|  4|        3|  3|
    |     6|   60| 0.75| 0.7142857142857143|  6|        4|  3|
    |     7|   90|  1.0| 0.8571428571428571|  7|        5|  4|
    |     8|   90|  1.0| 0.8571428571428571|  7|        5|  4|
    +------+-----+-----+-------------------+---+---------+---+
     */
    spark.stop()
  }

}
