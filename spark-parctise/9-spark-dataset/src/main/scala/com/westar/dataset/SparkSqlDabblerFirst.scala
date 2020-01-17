package com.westar.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlDabblerFirst {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("SparkSqlDabblerFirst")
      .getOrCreate()

    import spark.implicits._

    val df:DataFrame = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    //用sql的方式访问
    df.createOrReplaceTempView(("people"))

    val sqlDf:DataFrame = spark.sql("select name from people")
    sqlDf.show(truncate = false)


    //用DataFrame的api
    df.select("name").show()
    df.filter($"age" > 23).groupBy($"name").count().show()
    //df.select($"age" > 23).groupBy($"name").count()

    //用Dataset的api
    val ds:Dataset[Person] = df.as[Person]
    ds.map( p => p.name).show()
    ds.filter( p => p.age > 20)

    spark.stop()

  }

}
