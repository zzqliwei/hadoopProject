package com.westar.dataset.sql

import com.westar.dataset.Utils
import org.apache.spark.sql.SparkSession

object BasicSQLTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicSQLTest")
      .master("local")
      .getOrCreate()

    val sessionDf = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession")
    sessionDf.printSchema()
    sessionDf.createOrReplaceTempView("trackerSession")
    //查询所有记录
    val sessionRecords = spark.sql("SELECT * FROM trackerSession")
    sessionRecords.show()

    //过滤
    val filterSession =
      spark.sql("select distinct(cookie) from trackerSession where cookie_label = '固执'")
    filterSession.show()

    //先join， 然后再分组SQL
    val logDf = spark.read.parquet(s"${Utils.BASE_PATH}/trackerLog")
    logDf.printSchema()
    logDf.createOrReplaceTempView("trackerLog")

    val sql =
      """select tl.url, count(*) from trackerLog tl join trackerSession ts on tl.cookie = ts.cookie
        | where ts.cookie_label = '固执' and tl.log_type='pageview'
        | group by tl.url
        | order by tl.url desc
      """.stripMargin
    spark.sql(sql).show()

    //函数(内置函数)
    //单行函数
    spark.sql("select session_server_time,"+
      "hour(session_server_time) as hour from trackerSession").show(false)
    spark.sql("select click_count, cookie_label, " +
    "concat((click_count, cookie_label) as c from trackerSession").show()

    //多行函数(聚合函数)
    val ccmDF = spark.sql("select max(click_count) as ccm from trackerSession")
    ccmDF.printSchema()
    ccmDF.show()

    //udf
    spark.udf.register("myUDF",(arg1:Int,arg2:String) =>{
      if (arg1 > 1 && arg2.equals("固执")) {
        arg2 + arg1
      } else {
        arg2 + "less"
      }
    })

    spark.sql("select click_count, cookie_label, myUDF(click_count, cookie_label) as c"+
    "from trackerSession" ).show()

    spark.stop()
  }

}

