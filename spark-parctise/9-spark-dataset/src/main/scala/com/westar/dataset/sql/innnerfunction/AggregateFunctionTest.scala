package com.westar.dataset.sql.innnerfunction

import com.westar.dataset.TestData2
import org.apache.spark.sql.SparkSession

/**
 * 聚合函数
 *
 * approx_count_distinct
 * avg
 * corr
 * count
 * covar_pop
 * covar_samp
 * first
 * kurtosis
 * last
 * max
 * mean
 * min
 * percentile
 * percentile_approx
 * skewness
 * std
 * stddev_pop
 * sum
 * variance
 * collect_list
 * collect_set
 *
 */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AggregateFunctionTest")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil, 2).toDF()

    df.createOrReplaceTempView("testData2")

    //approx_count_distinct 返回count distinct的估计值，当数据量很大的时候可以近似估值
    spark.sql("select approx_count_distinct(a) from testData2")
    spark.sql("select approx_count_distinct(a, 0.4) from testData2")

    //avg 返回平均值
    spark.sql("select avg(a) from testData2").show()

    //corr(expr, expr) 返回两个expr的Pearson correlation
    //对于Pearson correlation，可以参考：http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
    spark.sql("select corr(a, b) from testData2").show() //输出：9.06493303673679E-17

    //count(expr)
    spark.sql("select count(a) from testData2").show()

    //covar_pop 用法covar_pop(expr1, expr2) - 返回两个表达式之间的总体协方差
    spark.sql("select covar_pop(a, b) from testData2").show()

    //covar_samp: 用法covar_samp(expr1, expr2) - 返回两个表达式之间的样本协方差
    spark.sql("select covar_samp(a, b) from testData2").show()

    //first : 用法first(expr[, isIgnoreNull]) - 返回表达式expr的第一个值，如果isIgnoreNull为true的话则返回非null的值
    //first_value和first是一样的
    spark.sql("select first(a) from testData2").show()

    //kurtosis: 用法kurtosis(expr) - 返回expr的kurtosis值 峰态系数 表征概率密度分布曲线
    spark.sql("select kurtosis(a) from testData2").show()

    //last : 用法last(expr[, isIgnoreNull]) - 返回表达式expr的最后一个值，如果isIgnoreNull为true的话则返回非null的值
    //last_value和last是一样的
    spark.sql("select last(a) from testData2").show() //输出：3

    //max: 用法max(expr) 返回expr的最大值
    spark.sql("select max(a) from testData2").show() //输出：3

    //mean: 用法mean(expr) 返回expr的平均值
    //和avg是一样的
    spark.sql("select mean(a) from testData2").show() //输出：2.0

    //min: 用法min(expr) 返回expr的最小值
    spark.sql("select min(a) from testData2").show() //输出：1

    //percentile: 用法percentile(expr, percentage) 返回expr中为percentage百分位的值 数据所在的位置
    spark.sql("select percentile(a, 0.5) from testData2").show() //输出：2
    spark.sql("select percentile(a, array(0.1, 0.2, 0.3, 0.5)) from testData2").show() //输出：[1.0, 1.0, 1.5, 2.0]

    //percentile_approx: 用法percentile_approx(expr, percentage[, accuracy]) 返回expr中为percentage百分位的近似值
    spark.sql("select percentile_approx(a, 0.5, 100) from testData2").show() //输出：2.0
    spark.sql("select percentile_approx(a, array(0.1, 0.2, 0.3, 0.5), 100) from testData2").show() //输出：[1.0, 1.0, 1.0, 2.0]

    //skewness : 用法skewness(expr) 返回expr的skewness值 偏度
    spark.sql("select skewness(a) from testData2").show() //输出：-5.09902483316444...

    //std : 用法std(expr) 返回expr的样本标准偏差值
    //stddev以及stddev_samp和std是一样的
    spark.sql("select std(a) from testData2").show() //输出：0.8944271909999159

    //stddev_pop : 用法stddev_pop(expr) 返回expr的总体标准偏差值
    spark.sql("select stddev_pop(a) from testData2").show() //输出：0.816496580927726

    //sum: 用法sum(expr) 返回expr的总值
    spark.sql("select sum(a) from testData2").show() //输出：12

    //variance: 用法variance(expr) 返回expr的样本方差
    //var_samp和variance一样
    spark.sql("select variance(a) from testData2").show() //输出：0.8

    //collect_list: 用法collect_list(expr) 返回由所有expr列组成的list
    spark.sql("select collect_list(a) from testData2").show() //输出：[1, 1, 2, 2, 3, 3]

    //collect_set: 用法collect_set(expr) 返回由所有expr列组成的set
    spark.sql("select collect_set(a) from testData2").show() //输出：[1, 2, 3]

    spark.stop()
  }

}
