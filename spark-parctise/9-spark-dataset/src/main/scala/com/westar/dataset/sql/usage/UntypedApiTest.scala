package com.westar.dataset.sql.usage

import com.westar.dataset.Person
import com.westar.dataset.Utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object UntypedApiTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UntypedApiTest")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val sessionDf = spark.read.parquet(s"${BASE_PATH}/trackerSession").as("ts")
    sessionDf.select("*").show()

    //过滤
    //Expression BuilderStyle
    val filterSession = sessionDf.filter($"cookie_label" === "固执").select("cookie")
    filterSession.show()

    //SQL Style
    val filterSessionSqlStyle = sessionDf.filter("cookie_label = '固执'").select("cookie")
    filterSessionSqlStyle.show()

    //关联分组查询
    val logDf = spark.read.parquet(s"${BASE_PATH}/trackerLog").as("tl")
    logDf.join(sessionDf,"cookie").where($"ts.cookie_label"=== "固执"
    and $"tl.log_type" === "pageview")
        .select("tl.url")
        .groupBy("tl.url").count()
        .orderBy($"url".desc).show()

    //函数
    import org.apache.spark.sql.functions._
    sessionDf.select(hour($"session_server_time")).show()
    sessionDf.select(concat($"session_server_time", $"cookie_label")).show()

    //udf
    spark.udf.register("myUDF",(arg1: Int, arg2: String) => arg2 + arg1)
    sessionDf.select(callUDF("myUDF",$"click_count", $"cookie_label")).show()

    val myUDF = udf {
      (arg1: Int, arg2: String) => arg2 + arg1
    }
    sessionDf.select(myUDF($"click_count", $"cookie_label")).show()


    //-------------------------------Column相关API start------------------------------//
    //别名
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    df.select(df("a").as("b")).columns.head //b
    df.select(df("a").alias("b")).columns.head //b
    df.select(df("a").name("b")).columns.head //b

    //cast
    sessionDf.select($"pageview_count".cast("int")).show()
    sessionDf.select($"pageview_count".cast(IntegerType)).show()

    //Column关系型运算
    sessionDf.select($"pageview_count" =!= 1).show()
    sessionDf.select($"pageview_count" > 1).show()
    sessionDf.select($"pageview_count" < 1).show()
    sessionDf.select($"pageview_count" <= 1).show()
    sessionDf.select($"pageview_count" >= 1).show()
    sessionDf.select($"cookie" <=> "cookie1").show() //相等测试，表示如果列$"cookie"的值为null的话，则返回false

    //case when字句
    sessionDf.select(when($"cookie_label" === "固执", 0)
      .when($"cookie_label" === "执着", 1).otherwise(2)).show()
    //between
    sessionDf.filter($"pageview_count".between(0, 2)).show()

    //判断是否为空
    sessionDf.filter($"cookie_label".isNaN).show()//判断是否为NaN
    sessionDf.filter($"cookie_label".isNull).show()
    sessionDf.filter($"cookie_label".isNotNull).show()

    //逻辑运算
    sessionDf.filter($"cookie_label" === "固执" || $"pageview_count" === 1).show()
    sessionDf.filter($"cookie_label" === "固执" && $"pageview_count" === 1).show()
    sessionDf.filter(!($"cookie_label" === "固执")).show()

    //数学运算
    sessionDf.select($"pageview_count" + 1).show()
    sessionDf.select($"pageview_count" - 1).show()
    sessionDf.select($"pageview_count" * 1).show()
    sessionDf.select($"pageview_count" / 1).show()
    sessionDf.select($"pageview_count" % 2).show()

    //isin
    sessionDf.filter($"cookie_label" isin ("固执", "执着", "不可理喻")).show()

    //like
    sessionDf.filter($"cookie_label" like "%固执%").show()
    sessionDf.filter($"cookie_label" rlike "%固执%").show() //LIKE with Regex

    sessionDf.orderBy($"cookie_label".desc)
    sessionDf.orderBy($"cookie_label".asc)

    //复杂类型的Column的访问
    val complexDF =
      Seq((Map("a" -> "b"), Seq(1, 2, 3), Person("jeffy", 30))).toDF("map", "seq", "case_class")
    complexDF.filter(complexDF("map").getItem("a") === "b").show()
    complexDF.filter(complexDF("seq").getItem(2) === 3).show()
    complexDF.filter(complexDF("case_class").getField("name") === "jeffy").show()

    //-------------------------------Column相关API end------------------------------//

    spark.stop()
  }

}
