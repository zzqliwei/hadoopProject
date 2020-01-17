package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * add_months
 * current_date
 * current_timestamp
 * datediff
 * date_add
 * date_sub
 * date_format
 * day
 * hour
 * last_day
 * minute
 * second
 * month
 * year
 * dayofyear
 * from_unixtime
 * from_utc_timestamp
 * months_between
 * next_day
 * quarter
 * to_date
 * to_unix_timestamp
 * to_utc_timestamp
 * trunc
 * unix_timestamp
 * weekofyear
 * window
 *
 *
 */
object DateFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("appName")
      .getOrCreate()


    //add_months 用法 add_months(start_date, num_months) 将日期start_date增加num_months个月
    spark.sql("select add_months('2016-08-31',1)").show()


    //current_date 用法 current_date() 返回这个sql执行的当前时间
    spark.sql("select current_date()").show()

    //current_timestamp 用法 current_timestamp() 返回这个sql执行的当前时间戳
    //now和current_timestamp功能是一样的
    spark.sql("select current_timestamp()").show()

    //datediff 用法 datediff(endDate, startDate) 返回endDate比startDate多多少天
    spark.sql("select datediff('2019-10-28','2019-10-27')").show()//1
    spark.sql("select datediff('2019-10-28','2019-10-29')").show()//-1

    //date_add 用法 date_add(start_date, num_days) 将日期start_date增加num_days天
    spark.sql("select date_add('2019-10-28',2)").show()

    //date_sub 用法 date_sub(start_date, num_days) 将日期start_date减掉num_days天
    spark.sql("select date_sub('2019-10-28',2)").show()


    //date_format 用法 date_format(date, fmt) 返回时间date指定的fmt格式
    spark.sql("select date_format('2019-10-28','y')").show()
    spark.sql("select date_format('2019-10-28','yyyy')").show()

    //day 用法 day(date) 返回date是在这个月中的第几天
    spark.sql("select day('2019-10-27')").show()

    //hour 用法 hour(timestamp) 返回timestamp中的第几个小时
    spark.sql("select hour(2019-10-27 20:30:33)").show()

    //last_day 用法 last_day(date) 返回date所在的月的最后一天的字符串日期
    spark.sql("select last_day('2019-10-28')").show()

    //minute 用法 minute(timestamp) 返回timestamp中的分钟的数值
    spark.sql("SELECT minute('2009-07-30 12:58:59')").show(false) //输出：30

    //second 用法 second(timestamp) 返回timestamp中的秒的数值
    spark.sql("SELECT second('2009-07-30 12:58:59')").show(false) //输出：59

    //month 用法 month(date) 返回date中的月份
    spark.sql("SELECT month('2016-07-30')").show(false) //输出：7

    //year 用法 year(date) 返回date中的年份
    spark.sql("SELECT year('2016-07-30')").show(false) //输出：2016

    //dayofyear 用法 dayofyear(date) 返回date是在这个年中的第几天
    spark.sql("select dayofyear('2019-10-29')").show()

    //from_unixtime 用法 from_unixtime(unix_time, format) 返回用format格式化的unix_time的字符串时间
    spark.sql("select from_unixtime(0,'yyyy-MM-dd HH:mm:ss')").show()

    //from_utc_timestamp 用法 from_utc_timestamp(timestamp, timezone) 将utc时间的timestamp计算并返回timestamp在timezone的时间
    spark.sql("select from_utc_timestamp('2019-10-28','Asia/Seoul')").show()

    //months_between 用法 months_between(timestamp1, timestamp2) 返回timestamp1和timestamp2之间有多少个月
    spark.sql("select months_between('2018-10-22','2019-10-12')").show()

    //next_day 用法 next_day(start_date, day_of_week) 返回start_date的下一个星期day_of_week
    spark.sql("select next_day('2019-11-13','TU')").show(false)

    //quarter 用法 quarter(date) 返回date所属的季节(用1,2,3,4表示)
    spark.sql("select quarter('2019-11-13')").show(false)

    //to_date 用法 to_date(expr) 返回expr的date时间
    spark.sql("select to_date('2019-11-13 22:30:50')").show(false) //009-07-30

    //to_unix_timestamp 用法 to_unix_timestamp(expr[, pattern]) 将pattern格式的expr转成时间戳
    spark.sql("select to_unix_timestamp('2019-11-12','yyyy-MM-dd')").show(false)

    //to_utc_timestamp 用法 to_utc_timestamp(timestamp, timezone) 将timezone时区的时间timestamp转成utc时区的timestamp
    spark.sql("select to_utc_timestamp('2019-11-13','Asia/Seoul')").show(false)

    //trunc 用法 trunc(date, fmt) 按照格式fmt将时间清零，只能清零月的和年的
    spark.sql("SELECT trunc('2009-02-12', 'MM')").show(false) //输出：2009-02-01
    spark.sql("SELECT trunc('2015-10-27', 'YEAR')").show(false) //输出：2015-01-01

    //unix_timestamp 用法 unix_timestamp([expr[, pattern]]) 返回当前的时间戳或者返回指定的时间的时间戳
    spark.sql("SELECT unix_timestamp()").show(false) //输出：1509376609
    spark.sql("SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd')").show(false) //输出：1460044800

    //weekofyear 用法 weekofyear(date) 返回指定日期date所在的当年中的第几个星期
    spark.sql("SELECT weekofyear('2008-02-20')").show(false) //输出：8


    //window 用法 window(expr) 返回expr的date时间
    //TODO 目前还不可以用
    spark.sql("SELECT window('2016-04-08', '10 second', '1 second', '0 second')").show(false) //输出：2009-07-30
    spark.stop()
  }

}
