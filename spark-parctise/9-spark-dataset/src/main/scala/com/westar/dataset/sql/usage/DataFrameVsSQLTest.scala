package com.westar.dataset.sql.usage

import org.apache.spark.sql.SparkSession
import com.westar.dataset.Utils._
object DataFrameVsSQLTest extends DFModule{
  val spark = SparkSession
    .builder()
    .appName("ActionApiTest")
    .getOrCreate()

  val personDf = spark.read.json(s"${BASE_PATH}/person.json")

  val temp = personDf.select("name")

  println(temp.schema) //可以加任何的代码

  temp.show()

  val df = cal(personDf) //模块化编程

  df.show()

  val groupByDf = temp.groupBy("name").count()

  groupByDf.show()

  spark.stop()

}
