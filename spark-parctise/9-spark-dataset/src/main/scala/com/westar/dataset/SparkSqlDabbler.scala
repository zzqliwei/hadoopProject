package com.westar.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlDabbler {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark,master")){
      conf.setMaster("local[*]")
    }
    val spark = SparkSession
      .builder()
      .appName("SparkSqlDabbler")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val jsonDataPath = s"${Utils.BASE_PATH}/people.json"
    //1: DataFrame和Dataset的创建
    val dataFrame:DataFrame = spark.read.json(jsonDataPath)

    //2: DataFrame和Dataset的互转
    val dataset:Dataset[Person] = dataFrame.as[Person]

    //3: DataFrame和Dataset的schema的定义以及使用
    dataFrame.schema
    dataFrame.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)


    dataset.schema
    dataset.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    //4: 用API调用并运行sql
    dataFrame.createOrReplaceTempView("people")
    dataset.createOrReplaceTempView("people")

    val sqlDf = spark.sql("select age, count(*) as cnt from people where age > 21 group by age")
    sqlDf.printSchema()
    sqlDf.show()

    //5: DataFrame的api的使用
    dataFrame.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |29|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    dataFrame.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    import spark.implicits._
    // 查询所有列数据，并且age列都需要加上1
    dataFrame.select($"name",$"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     30|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // 查询age > 21的人
    dataFrame.filter($"age" > 21).show()
    /*
   +---+-------+
 |age|   name|
 +---+-------+
 | 29|Michael|
 | 30|   Andy|
 +---+-------+
    */
    // group by age and count
    dataFrame.groupBy($"age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |29|    1|
    // |  30|    1|
    // +----+-----+

    //6: Dataset的api的使用
    dataset.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |29|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    //支持DSL编程
    val nameDf:Dataset[Row] = dataset.select("name")
    nameDf.show()

    val nameDs:Dataset[String] = dataset.select($"name".as[String])
    nameDs.show()

    //同时支持函数式编程
    val groupByKeyDs = dataset.map(p =>{
      if(p.age > 10){
        Person(p.name, 10)
      } else p
    }).groupByKey(p =>{
      p.name
    })

    groupByKeyDs.count().show()

    spark.stop()
  }

}
